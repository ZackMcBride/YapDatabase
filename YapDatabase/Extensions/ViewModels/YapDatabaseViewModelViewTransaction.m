#import "YapDatabaseViewModelViewTransaction.h"
#import "YapDatabaseViewModel.h"
#import "YapDatabaseViewPrivate.h"
#import "YapCollectionKey.h"
#import "YapDatabaseTransaction.h"
#import "YapDatabasePrivate.h"
#import "YapDatabaseString.h"
#import "YapNull.h"
#import "YapDatabaseLogging.h"

#if DEBUG
    static const int ydbLogLevel = YDB_LOG_LEVEL_INFO;
#else
    static const int ydbLogLevel = YDB_LOG_LEVEL_WARN;
#endif

@implementation YapDatabaseViewModelViewTransaction {
    sqlite3_stmt *getRowidForKeyStatement;
    sqlite3_stmt *enumerateCollectionsStatement;
    sqlite3_stmt *enumerateRowsInCollectionStatement;
    sqlite3_stmt *getKeyForRowidStatement;
    sqlite3_stmt *getDataForRowidStatement;
    sqlite3_stmt *getMetadataForRowidStatement;
    NSString *viewModelTableName;
    NSString *viewModelName;
}

- (instancetype)initWithViewConnection:(id)aViewConnection databaseTransaction:(YapDatabaseReadTransaction *)aDatabaseTransaction viewModelName:(NSString *)aViewModelName {
    self = [self initWithViewConnection:aViewConnection databaseTransaction:aDatabaseTransaction];
    if (self) {
        viewModelTableName = [[YapDatabaseViewModel tableNameForRegisteredName:aViewModelName] copy];
        viewModelName = [aViewModelName copy];
    }
    return self;
}

- (id)our_metadataForCollectionKey:(YapCollectionKey *)cacheKey withRowid:(int64_t)rowid
{
    if (cacheKey == nil) return nil;

	id metadata;// = [databaseTransaction->connection->metadataCache objectForKey:cacheKey];
//	if (metadata)
//		return metadata;

	sqlite3_stmt *statement = [self getMetadataForRowidStatement];
	if (statement == NULL) return nil;

	// SELECT "metadata" FROM "database2" WHERE "rowid" = ?;

	sqlite3_bind_int64(statement, 1, rowid);

	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (databaseTransaction->connection->needsMarkSqlLevelSharedReadLock)
			[databaseTransaction->connection markSqlLevelSharedReadLockAcquired];

		const void *blob = sqlite3_column_blob(statement, 0);
		int blobSize = sqlite3_column_bytes(statement, 0);

		// Performance tuning:
		// Use dataWithBytesNoCopy to avoid an extra allocation and memcpy.

		NSData *data = [NSData dataWithBytesNoCopy:(void *)blob length:blobSize freeWhenDone:NO];
		metadata = databaseTransaction->connection->database->metadataDeserializer(cacheKey.collection, cacheKey.key, data);
//
//		if (metadata)
//			[databaseTransaction->connection->metadataCache setObject:metadata forKey:cacheKey];
	}
	else if (status == SQLITE_ERROR)
	{
		YDBLogError(@"Error executing 'getMetadataForRowidStatement': %d %s", status, sqlite3_errmsg(databaseTransaction->connection->db));
	}

	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);

	return metadata;
}

- (YapCollectionKey *)our_collectionKeyForRowid:(int64_t)rowid
{
//    NSNumber *rowidNumber = @(rowid);

	YapCollectionKey *collectionKey;// = [databaseTransaction->connection->keyCache objectForKey:rowidNumber];
//	if (collectionKey)
//	{
//		return collectionKey;
//	}

	sqlite3_stmt *statement = [self getKeyForRowidStatement];
	if (statement == NULL) {
		return nil;
	}

	// SELECT "collection", "key" FROM "database2" WHERE "rowid" = ?;

	sqlite3_bind_int64(statement, 1, rowid);

	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (databaseTransaction->connection->needsMarkSqlLevelSharedReadLock)
			[databaseTransaction->connection markSqlLevelSharedReadLockAcquired];

		const unsigned char *text1 = sqlite3_column_text(statement, 0);
		int textSize1 = sqlite3_column_bytes(statement, 0);

		NSString *collection = viewModelName;
		NSString *key        = [[NSString alloc] initWithBytes:text1 length:textSize1 encoding:NSUTF8StringEncoding];

		collectionKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];

		//[databaseTransaction->connection->keyCache setObject:collectionKey forKey:rowidNumber];
	}
	else if (status == SQLITE_ERROR)
	{
		YDBLogError(@"Error executing 'getKeyForRowidStatement': %d %s", status, sqlite3_errmsg(databaseTransaction->connection->db));
	}

	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);

	return collectionKey;
}

- (id)our_objectForCollectionKey:(YapCollectionKey *)cacheKey withRowid:(int64_t)rowid
{
    if (cacheKey == nil) return nil;

	id object;// = [databaseTransaction->connection->objectCache objectForKey:cacheKey];
//	if (object)
//		return object;

	sqlite3_stmt *statement = [self getDataForRowidStatement];
	if (statement == NULL) return nil;

	// SELECT "data" FROM "database2" WHERE "rowid" = ?;

	sqlite3_bind_int64(statement, 1, rowid);

	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (databaseTransaction->connection->needsMarkSqlLevelSharedReadLock)
			[databaseTransaction->connection markSqlLevelSharedReadLockAcquired];

		const void *blob = sqlite3_column_blob(statement, 0);
		int blobSize = sqlite3_column_bytes(statement, 0);

		// Performance tuning:
		// Use dataWithBytesNoCopy to avoid an extra allocation and memcpy.

		NSData *data = [NSData dataWithBytesNoCopy:(void *)blob length:blobSize freeWhenDone:NO];
		object = databaseTransaction->connection->database->objectDeserializer(cacheKey.collection, cacheKey.key, data);
//
//		if (object)
//			[databaseTransaction->connection->objectCache setObject:object forKey:cacheKey];
	}
	else if (status == SQLITE_ERROR)
	{
		YDBLogError(@"Error executing 'getDataForRowidStatement': %d %s", status, sqlite3_errmsg(databaseTransaction->connection->db));
	}

	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);

	return object;
}

- (BOOL)our_getCollectionKey:(YapCollectionKey **)collectionKeyPtr object:(id *)objectPtr forRowid:(int64_t)rowid
{
    YapCollectionKey *collectionKey = [self our_collectionKeyForRowid:rowid];
	if (collectionKey)
	{
		id object = [self our_objectForCollectionKey:collectionKey withRowid:rowid];

		if (collectionKeyPtr) *collectionKeyPtr = collectionKey;
		if (objectPtr) *objectPtr = object;
		return YES;
	}
	else
	{
		if (collectionKeyPtr) *collectionKeyPtr = nil;
		if (objectPtr) *objectPtr = nil;
		return NO;
	}
}

- (BOOL)our_getCollectionKey:(YapCollectionKey **)collectionKeyPtr metadata:(id *)metadataPtr forRowid:(int64_t)rowid
{
    YapCollectionKey *collectionKey = [self our_collectionKeyForRowid:rowid];
	if (collectionKey)
	{
		id metadata = [self our_metadataForCollectionKey:collectionKey withRowid:rowid];

		if (collectionKeyPtr) *collectionKeyPtr = collectionKey;
		if (metadataPtr) *metadataPtr = metadata;
		return YES;
	}
	else
	{
		if (collectionKeyPtr) *collectionKeyPtr = nil;
		if (metadataPtr) *metadataPtr = nil;
		return NO;
	}
}

- (BOOL)our_getCollectionKey:(YapCollectionKey **)collectionKeyPtr
                      object:(id *)objectPtr
                    metadata:(id *)metadataPtr
                    forRowid:(int64_t)rowid
{
    YapCollectionKey *collectionKey = [self our_collectionKeyForRowid:rowid];
	if (collectionKey == nil)
	{
		if (collectionKeyPtr) *collectionKeyPtr = nil;
		if (objectPtr) *objectPtr = nil;
		if (metadataPtr) *metadataPtr = nil;
		return NO;
	}

	if ([self our_getObject:objectPtr metadata:metadataPtr forCollectionKey:collectionKey withRowid:rowid])
	{
		if (collectionKeyPtr) *collectionKeyPtr = collectionKey;
		return YES;
	}
	else
	{
		if (collectionKeyPtr) *collectionKeyPtr = nil;
		return NO;
	}
}

- (BOOL)our_getRowid:(int64_t *)rowidPtr forKey:(NSString *)key inCollection:(NSString *)collection
{
    if (key == nil) {
		if (rowidPtr) *rowidPtr = 0;
		return NO;
	}
	if (collection == nil) collection = @"";

	sqlite3_stmt *statement = [self getRowidForKeyStatement];
	if (statement == NULL) {
		if (rowidPtr) *rowidPtr = 0;
		return NO;
	}

	// SELECT "rowid" FROM [viewModelTableName] WHERE "collection" = ? AND "key" = ?;

//	YapDatabaseString _collection; MakeYapDatabaseString(&_collection, collection);
//	sqlite3_bind_text(statement, 1, _collection.str, _collection.length, SQLITE_STATIC);

	YapDatabaseString _key; MakeYapDatabaseString(&_key, key);
	sqlite3_bind_text(statement, 2, _key.str, _key.length,  SQLITE_STATIC);

	int64_t rowid = 0;
	BOOL result = NO;

	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (databaseTransaction->connection->needsMarkSqlLevelSharedReadLock)
			[databaseTransaction->connection markSqlLevelSharedReadLockAcquired];

		rowid = sqlite3_column_int64(statement, 0);
		result = YES;
	}
	else if (status == SQLITE_ERROR)
	{
		YDBLogError(@"Error executing 'getRowidForKeyStatement': %d %s", status, sqlite3_errmsg(databaseTransaction->connection->db));
	}

	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
//	FreeYapDatabaseString(&_collection);
	FreeYapDatabaseString(&_key);

	if (rowidPtr) *rowidPtr = rowid;
	return result;
}

- (void)our_enumerateCollectionsUsingBlock:(void (^)(NSString *collection, BOOL *stop))block
{
    if (block == NULL) return;
    BOOL stop = YES;
	block(nil, &stop);
}

- (void)our__enumerateRowsInCollections:(NSArray *)collections usingBlock:
(void (^)(int64_t rowid, NSString *collection, NSString *key, id object, id metadata, BOOL *stop))block
{
    [self our__enumerateRowsInCollections:collections usingBlock:block withFilter:NULL];
}

- (void)our__enumerateRowsInAllCollectionsUsingBlock:
(void (^)(int64_t rowid, NSString *collection, NSString *key, id object, id metadata, BOOL *stop))block
{
    [self our__enumerateRowsInAllCollectionsUsingBlock:block withFilter:NULL];
}

- (void)our__enumerateRowsInCollections:(NSArray *)collections
                             usingBlock:(void (^)(int64_t rowid, NSString *collection, NSString *key, id object, id metadata, BOOL *stop))block
                             withFilter:(BOOL (^)(int64_t rowid, NSString *collection, NSString *key))filter
{
    if (block == NULL) return;
	if ([collections count] == 0) return;

	sqlite3_stmt *statement = [self enumerateRowsInCollectionStatement];
	if (statement == NULL) return;

	databaseTransaction->isMutated = NO; // mutation during enumeration protection
	BOOL stop = NO;

	BOOL unlimitedObjectCacheLimit = (databaseTransaction->connection->objectCacheLimit == 0);
	BOOL unlimitedMetadataCacheLimit = (databaseTransaction->connection->metadataCacheLimit == 0);

	// SELECT "rowid", "key", "data", "metadata" FROM "database2" WHERE "collection" = ?;
	//
	// Performance tuning:
	// Use dataWithBytesNoCopy to avoid an extra allocation and memcpy.
	//
	// Cache considerations:
	// Do we want to add the objects/metadata to the cache here?
	// If the cache is unlimited then we should.
	// Otherwise we should only add to the cache if its not full.
	// The cache should generally be reserved for items that are explicitly fetched,
	// and we don't want to crowd them out during enumerations.

	for (NSString *collection in collections)
	{
//		YapDatabaseString _collection; MakeYapDatabaseString(&_collection, collection);
//		sqlite3_bind_text(statement, 1, _collection.str, _collection.length, SQLITE_STATIC);

		int status = sqlite3_step(statement);
		if (status == SQLITE_ROW)
		{
			if (databaseTransaction->connection->needsMarkSqlLevelSharedReadLock)
				[databaseTransaction->connection markSqlLevelSharedReadLockAcquired];

			do
			{
				int64_t rowid = sqlite3_column_int64(statement, 0);

				const unsigned char *text = sqlite3_column_text(statement, 1);
				int textSize = sqlite3_column_bytes(statement, 1);

				NSString *key = [[NSString alloc] initWithBytes:text length:textSize encoding:NSUTF8StringEncoding];

				BOOL invokeBlock = (filter == NULL) ? YES : filter(rowid, collection, key);
				if (invokeBlock)
				{
					YapCollectionKey *cacheKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];

					id object;// = [databaseTransaction->connection->objectCache objectForKey:cacheKey];
					if (object == nil)
					{
						const void *oBlob = sqlite3_column_blob(statement, 2);
						int oBlobSize = sqlite3_column_bytes(statement, 2);

						NSData *oData = [NSData dataWithBytesNoCopy:(void *)oBlob length:oBlobSize freeWhenDone:NO];
						object = databaseTransaction->connection->database->objectDeserializer(collection, key, oData);

//						if (unlimitedObjectCacheLimit ||
//						    [databaseTransaction->connection->objectCache count] < databaseTransaction->connection->objectCacheLimit)
//						{
//							if (object)
//								[databaseTransaction->connection->objectCache setObject:object forKey:cacheKey];
//						}
					}

					id metadata;// = [databaseTransaction->connection->metadataCache objectForKey:cacheKey];
					if (metadata)
					{
						if (metadata == [YapNull null])
							metadata = nil;
					}
					else
					{
						const void *mBlob = sqlite3_column_blob(statement, 3);
						int mBlobSize = sqlite3_column_bytes(statement, 3);

						if (mBlobSize > 0)
						{
							NSData *mData = [NSData dataWithBytesNoCopy:(void *)mBlob length:mBlobSize freeWhenDone:NO];
							metadata = databaseTransaction->connection->database->metadataDeserializer(collection, key, mData);
						}

//						if (unlimitedMetadataCacheLimit ||
//						    [databaseTransaction->connection->metadataCache count] < databaseTransaction->connection->metadataCacheLimit)
//						{
//							if (metadata)
//								[databaseTransaction->connection->metadataCache setObject:metadata forKey:cacheKey];
//							else
//								[databaseTransaction->connection->metadataCache setObject:[YapNull null] forKey:cacheKey];
//						}
					}

					block(rowid, collection, key, object, metadata, &stop);

					if (stop || databaseTransaction->isMutated) break;
				}

			} while ((status = sqlite3_step(statement)) == SQLITE_ROW);
		}

		if ((status != SQLITE_DONE) && !stop && !databaseTransaction->isMutated)
		{
			YDBLogError(@"%@ - sqlite_step error: %d %s", THIS_METHOD, status, sqlite3_errmsg(databaseTransaction->connection->db));
		}

		sqlite3_clear_bindings(statement);
		sqlite3_reset(statement);
//		FreeYapDatabaseString(&_collection);

		if (databaseTransaction->isMutated && !stop)
		{
			@throw [databaseTransaction mutationDuringEnumerationException];
		}

		if (stop)
		{
			break;
		}

	} // end for (NSString *collection in collections)
}

- (void)our__enumerateRowsInAllCollectionsUsingBlock:
(void (^)(int64_t rowid, NSString *collection, NSString *key, id object, id metadata, BOOL *stop))block
                                          withFilter:(BOOL (^)(int64_t rowid, NSString *collection, NSString *key))filter
{
    [self our__enumerateRowsInAllCollectionsUsingBlock:block withFilter:filter];
}

- (void)our__enumerateKeysAndObjectsInCollections:(NSArray *)collections usingBlock:
(void (^)(int64_t rowid, NSString *collection, NSString *key, id object, BOOL *stop))block
{
    [self our__enumerateKeysAndObjectsInCollections:collections usingBlock:block];
}

- (void)our__enumerateKeysAndObjectsInAllCollectionsUsingBlock:
(void (^)(int64_t rowid, NSString *collection, NSString *key, id object, BOOL *stop))block
{
    [self our__enumerateKeysAndObjectsInAllCollectionsUsingBlock:block];
}

- (void)our__enumerateKeysAndObjectsInCollections:(NSArray *)collections
                                       usingBlock:(void (^)(int64_t rowid, NSString *collection, NSString *key, id object, BOOL *stop))block
                                       withFilter:(BOOL (^)(int64_t rowid, NSString *collection, NSString *key))filter
{
    [self our__enumerateKeysAndObjectsInCollections:collections usingBlock:block withFilter:filter];
}

- (void)our__enumerateKeysAndObjectsInAllCollectionsUsingBlock:
(void (^)(int64_t rowid, NSString *collection, NSString *key, id object, BOOL *stop))block
                                                    withFilter:(BOOL (^)(int64_t rowid, NSString *collection, NSString *key))filter
{
    [self our__enumerateKeysAndObjectsInAllCollectionsUsingBlock:block withFilter:filter];
}

- (void)our__enumerateKeysAndMetadataInCollections:(NSArray *)collections
                                        usingBlock:(void (^)(int64_t rowid, NSString *collection, NSString *key, id metadata, BOOL *stop))block
{
    [self our__enumerateKeysAndMetadataInCollections:collections usingBlock:block];
}


- (void)our__enumerateKeysAndMetadataInCollections:(NSArray *)collections
                                        usingBlock:(void (^)(int64_t rowid, NSString *collection, NSString *key, id metadata, BOOL *stop))block
                                        withFilter:(BOOL (^)(int64_t rowid, NSString *collection, NSString *key))filter
{
    [self our__enumerateKeysAndMetadataInCollections:collections usingBlock:block withFilter:filter];
}

- (void)our__enumerateKeysAndMetadataInAllCollectionsUsingBlock:
(void (^)(int64_t rowid, NSString *collection, NSString *key, id metadata, BOOL *stop))block
{
    [self our__enumerateKeysAndMetadataInAllCollectionsUsingBlock:block];
}

- (void)our__enumerateKeysAndMetadataInAllCollectionsUsingBlock:
(void (^)(int64_t rowid, NSString *collection, NSString *key, id metadata, BOOL *stop))block
                                                     withFilter:(BOOL (^)(int64_t rowid, NSString *collection, NSString *key))filter
{
    [self our__enumerateKeysAndMetadataInAllCollectionsUsingBlock:block withFilter:filter];
}

- (void)our__enumerateKeysInCollections:(NSArray *)collections
                             usingBlock:(void (^)(int64_t rowid, NSString *collection, NSString *key, BOOL *stop))block
{
    [self our__enumerateKeysInCollections:collections usingBlock:block];
}

- (void)our__enumerateKeysInAllCollectionsUsingBlock:
(void (^)(int64_t rowid, NSString *collection, NSString *key, BOOL *stop))block
{
    [self our__enumerateKeysInAllCollectionsUsingBlock:block];
}

- (BOOL)our_getObject:(id *)objectPtr
             metadata:(id *)metadataPtr
     forCollectionKey:(YapCollectionKey *)collectionKey
            withRowid:(int64_t)rowid
{
    return [self our_getObject:objectPtr metadata:metadataPtr forCollectionKey:collectionKey withRowid:rowid];
}

- (sqlite3_stmt *)getRowidForKeyStatement
{
	sqlite3_stmt **statement = &getRowidForKeyStatement;
	if (*statement == NULL)
	{
        NSString *q = [NSString stringWithFormat:@"SELECT \"rowid\" FROM \"%@\" WHERE \"key\" = ?;", viewModelTableName];
		const char *stmt = q.UTF8String;
		int stmtLen = (int)strlen(stmt);

		int status = sqlite3_prepare_v2(databaseTransaction->connection->db, stmt, stmtLen+1, statement, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"Error creating '%@': %d %s", THIS_METHOD, status, sqlite3_errmsg(databaseTransaction->connection->db));
		}
	}

	return *statement;
}

- (sqlite3_stmt *)enumerateRowsInCollectionStatement
{
	sqlite3_stmt **statement = &enumerateRowsInCollectionStatement;
	if (*statement == NULL)
	{
        NSString *q = [NSString stringWithFormat:@"SELECT \"rowid\", \"key\", \"data\", \"metadata\" FROM \"%@\";", viewModelTableName];

		const char *stmt = q.UTF8String;
		int stmtLen = (int)strlen(stmt);

		int status = sqlite3_prepare_v2(databaseTransaction->connection->db, stmt, stmtLen+1, statement, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"Error creating '%@': %d %s", THIS_METHOD, status, sqlite3_errmsg(databaseTransaction->connection->db));
		}
	}

	return *statement;
}

- (sqlite3_stmt *)getKeyForRowidStatement
{
	sqlite3_stmt **statement = &getKeyForRowidStatement;
	if (*statement == NULL)
	{
        NSString *q = [NSString stringWithFormat:@"SELECT \"key\" FROM \"%@\" WHERE \"rowid\" = ?;", viewModelTableName];
		const char *stmt = q.UTF8String;
		int stmtLen = (int)strlen(stmt);

		int status = sqlite3_prepare_v2(databaseTransaction->connection->db, stmt, stmtLen+1, statement, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"Error creating '%@': %d %s", THIS_METHOD, status, sqlite3_errmsg(databaseTransaction->connection->db));
		}
	}

	return *statement;
}

- (sqlite3_stmt *)getDataForRowidStatement
{
	sqlite3_stmt **statement = &getDataForRowidStatement;
	if (*statement == NULL)
	{
        NSString *q = [NSString stringWithFormat:@"SELECT \"data\" FROM \"%@\" WHERE \"rowid\" = ?;", viewModelTableName];
		const char *stmt = q.UTF8String;
		int stmtLen = (int)strlen(stmt);

		int status = sqlite3_prepare_v2(databaseTransaction->connection->db, stmt, stmtLen+1, statement, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"Error creating '%@': %d %s", THIS_METHOD, status, sqlite3_errmsg(databaseTransaction->connection->db));
		}
	}

	return *statement;
}

- (sqlite3_stmt *)getMetadataForRowidStatement
{
	sqlite3_stmt **statement = &getMetadataForRowidStatement;
	if (*statement == NULL)
	{
        NSString *q = [NSString stringWithFormat:@"SELECT \"metadata\" FROM \"%@\" WHERE \"rowid\" = ?;", viewModelTableName];
		const char *stmt = q.UTF8String;
		int stmtLen = (int)strlen(stmt);

		int status = sqlite3_prepare_v2(databaseTransaction->connection->db, stmt, stmtLen+1, statement, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"Error creating '%@': %d %s", THIS_METHOD, status, sqlite3_errmsg(databaseTransaction->connection->db));
		}
	}

	return *statement;
}

@end
