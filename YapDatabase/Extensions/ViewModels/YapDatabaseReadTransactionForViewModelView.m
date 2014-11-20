#import "YapDatabaseReadTransactionForViewModelView.h"

#import "YapDatabaseViewModel.h"
#import "YapDatabasePrivate.h"
#import "YapDatabaseString.h"
#import "YapNull.h"
#import "YapDatabaseLogging.h"

#if DEBUG
static const int ydbLogLevel = YDB_LOG_LEVEL_INFO;
#else
static const int ydbLogLevel = YDB_LOG_LEVEL_WARN;
#endif

@interface YapDatabaseReadTransactionForViewModelView ()

@property (nonatomic, readonly) NSString *viewModelName;
@property (nonatomic, readonly) NSString *viewModelTableName;
@property (nonatomic, strong) YapCache *keyCache;
@property (nonatomic, strong) YapCache *objectCache;
@property (nonatomic, strong) YapCache *metadataCache;

@end

@implementation YapDatabaseReadTransactionForViewModelView {
    sqlite3_stmt *getRowidForKeyStatement;
    sqlite3_stmt *enumerateCollectionsStatement;
    sqlite3_stmt *enumerateRowsInCollectionStatement;
    sqlite3_stmt *getKeyForRowidStatement;
    sqlite3_stmt *getDataForRowidStatement;
    sqlite3_stmt *getMetadataForRowidStatement;
    sqlite3_stmt *enumerateKeysInCollectionStatement;
	sqlite3_stmt *enumerateKeysInAllCollectionsStatement;
	sqlite3_stmt *enumerateKeysAndMetadataInCollectionStatement;
	sqlite3_stmt *enumerateKeysAndMetadataInAllCollectionsStatement;
	sqlite3_stmt *enumerateKeysAndObjectsInCollectionStatement;
	sqlite3_stmt *enumerateKeysAndObjectsInAllCollectionsStatement;
	sqlite3_stmt *enumerateRowsInAllCollectionsStatement;
    sqlite3_stmt *getDataForKeyStatement;
    sqlite3_stmt *getAllForKeyStatement;
    sqlite3_stmt *getAllForRowidStatement;
}

- (instancetype)initWithViewModelName:(NSString *)viewModelName {
    self = [super init];
    if (self) {
        _viewModelName = viewModelName;
        _viewModelTableName = [YapDatabaseViewModel tableNameForRegisteredName:viewModelName];
        YapDatabaseConnectionDefaults *defaults = [connection->database connectionDefaults];
        _keyCache = [[YapCache alloc] initWithKeyClass:[YapCollectionKey class]
                                          keyCallbacks:[YapCollectionKey keyCallbacks]
                                            countLimit:defaults.objectCacheLimit];
        _objectCache = [[YapCache alloc] initWithKeyClass:[YapCollectionKey class]
                                             keyCallbacks:[YapCollectionKey keyCallbacks]
                                               countLimit:defaults.objectCacheLimit];
        _metadataCache = [[YapCache alloc] initWithKeyClass:[YapCollectionKey class]
                                               keyCallbacks:[YapCollectionKey keyCallbacks]
                                                 countLimit:defaults.metadataCacheLimit];
    }
    return self;
}

- (YapCollectionKey *)collectionKeyForRowid:(int64_t)rowid {
    NSNumber *rowidNumber = @(rowid);

	YapCollectionKey *collectionKey = [self.keyCache objectForKey:rowidNumber];
    if (collectionKey)
    {
        return collectionKey;
    }

	sqlite3_stmt *statement = [self getKeyForRowidStatement];
	if (statement == NULL) {
		return nil;
	}

	// SELECT "collection", "key" FROM "database2" WHERE "rowid" = ?;

	sqlite3_bind_int64(statement, 1, rowid);

	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (connection->needsMarkSqlLevelSharedReadLock)
			[connection markSqlLevelSharedReadLockAcquired];

		const unsigned char *text1 = sqlite3_column_text(statement, 0);
		int textSize1 = sqlite3_column_bytes(statement, 0);

		NSString *collection = self.viewModelName;
		NSString *key        = [[NSString alloc] initWithBytes:text1 length:textSize1 encoding:NSUTF8StringEncoding];

		collectionKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];

		[self.keyCache setObject:collectionKey forKey:rowidNumber];
	}
	else if (status == SQLITE_ERROR)
	{
		YDBLogError(@"Error executing 'getKeyForRowidStatement': %d %s", status, sqlite3_errmsg(connection->db));
	}

	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);

	return collectionKey;
}

- (BOOL)getCollectionKey:(YapCollectionKey **)collectionKeyPtr object:(id *)objectPtr forRowid:(int64_t)rowid {
    YapCollectionKey *collectionKey = [self collectionKeyForRowid:rowid];
	if (collectionKey)
	{
		id object = [self objectForCollectionKey:collectionKey withRowid:rowid];

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

- (BOOL)getCollectionKey:(YapCollectionKey **)collectionKeyPtr metadata:(id *)metadataPtr forRowid:(int64_t)rowid {
    YapCollectionKey *collectionKey = [self collectionKeyForRowid:rowid];
	if (collectionKey)
	{
		id metadata = [self metadataForCollectionKey:collectionKey withRowid:rowid];

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

- (BOOL)getCollectionKey:(YapCollectionKey **)collectionKeyPtr object:(id *)objectPtr metadata:(id *)metadataPtr forRowid:(int64_t)rowid {
    YapCollectionKey *collectionKey = [self collectionKeyForRowid:rowid];
	if (collectionKey == nil)
	{
		if (collectionKeyPtr) *collectionKeyPtr = nil;
		if (objectPtr) *objectPtr = nil;
		if (metadataPtr) *metadataPtr = nil;
		return NO;
	}

	if ([self getObject:objectPtr metadata:metadataPtr forCollectionKey:collectionKey withRowid:rowid])
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

- (BOOL)getRowid:(int64_t *)rowidPtr forKey:(NSString *)key inCollection:(NSString *)collection {
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

	// SELECT "rowid" FROM [viewModelTableName] WHERE "key" = ?;


	YapDatabaseString _key; MakeYapDatabaseString(&_key, key);
	sqlite3_bind_text(statement, 2, _key.str, _key.length,  SQLITE_STATIC);

	int64_t rowid = 0;
	BOOL result = NO;

	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (connection->needsMarkSqlLevelSharedReadLock)
			[connection markSqlLevelSharedReadLockAcquired];

		rowid = sqlite3_column_int64(statement, 0);
		result = YES;
	}
	else if (status == SQLITE_ERROR)
	{
		YDBLogError(@"Error executing 'getRowidForKeyStatement': %d %s", status, sqlite3_errmsg(connection->db));
	}

	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	FreeYapDatabaseString(&_key);

	if (rowidPtr) *rowidPtr = rowid;
	return result;
}

- (id)objectForCollectionKey:(YapCollectionKey *)cacheKey withRowid:(int64_t)rowid {
    if (cacheKey == nil) return nil;

	id object = [self.objectCache objectForKey:cacheKey];
    if (object)
        return object;

	sqlite3_stmt *statement = [self getDataForRowidStatement];
	if (statement == NULL) return nil;

	// SELECT "data" FROM "view-model-table" WHERE "rowid" = ?;

	sqlite3_bind_int64(statement, 1, rowid);

	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (connection->needsMarkSqlLevelSharedReadLock)
			[connection markSqlLevelSharedReadLockAcquired];

		const void *blob = sqlite3_column_blob(statement, 0);
		int blobSize = sqlite3_column_bytes(statement, 0);

		// Performance tuning:
		// Use dataWithBytesNoCopy to avoid an extra allocation and memcpy.

		NSData *data = [NSData dataWithBytesNoCopy:(void *)blob length:blobSize freeWhenDone:NO];
		object = connection->database->objectDeserializer(cacheKey.collection, cacheKey.key, data);

        if (object)
            [self.objectCache setObject:object forKey:cacheKey];
	}
	else if (status == SQLITE_ERROR)
	{
		YDBLogError(@"Error executing 'getDataForRowidStatement': %d %s", status, sqlite3_errmsg(connection->db));
	}

	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);

	return object;
}

- (id)metadataForCollectionKey:(YapCollectionKey *)cacheKey withRowid:(int64_t)rowid {
    if (cacheKey == nil) return nil;

	id metadata = [self.metadataCache objectForKey:cacheKey];
    if (metadata)
        return metadata;

	sqlite3_stmt *statement = [self getMetadataForRowidStatement];
	if (statement == NULL) return nil;

	// SELECT "metadata" FROM "view-model-table" WHERE "rowid" = ?;

	sqlite3_bind_int64(statement, 1, rowid);

	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (connection->needsMarkSqlLevelSharedReadLock)
			[connection markSqlLevelSharedReadLockAcquired];

		const void *blob = sqlite3_column_blob(statement, 0);
		int blobSize = sqlite3_column_bytes(statement, 0);

		// Performance tuning:
		// Use dataWithBytesNoCopy to avoid an extra allocation and memcpy.

		NSData *data = [NSData dataWithBytesNoCopy:(void *)blob length:blobSize freeWhenDone:NO];
		metadata = connection->database->metadataDeserializer(cacheKey.collection, cacheKey.key, data);

        if (metadata)
            [self.metadataCache setObject:metadata forKey:cacheKey];
	}
	else if (status == SQLITE_ERROR)
	{
		YDBLogError(@"Error executing 'getMetadataForRowidStatement': %d %s", status, sqlite3_errmsg(connection->db));
	}

	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);

	return metadata;
}

- (id)objectForKey:(NSString *)key inCollection:(NSString *)collection {
	if (key == nil) return nil;
	if (collection == nil) collection = @"";

	YapCollectionKey *cacheKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];

	id object = [self.objectCache objectForKey:cacheKey];
	if (object)
		return object;

	sqlite3_stmt *statement = [self getDataForKeyStatement];
	if (statement == NULL) return nil;

	// SELECT "data" FROM "view-model-table" WHERE "key" = ?;

	YapDatabaseString _key; MakeYapDatabaseString(&_key, key);
	sqlite3_bind_text(statement, 1, _key.str, _key.length, SQLITE_STATIC);

	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (connection->needsMarkSqlLevelSharedReadLock)
			[connection markSqlLevelSharedReadLockAcquired];

		const void *blob = sqlite3_column_blob(statement, 0);
		int blobSize = sqlite3_column_bytes(statement, 0);

		// Performance tuning:
		// Use dataWithBytesNoCopy to avoid an extra allocation and memcpy.
		// Be sure not to call sqlite3_reset until we're done with the data.

		NSData *data = [NSData dataWithBytesNoCopy:(void *)blob length:blobSize freeWhenDone:NO];
		object = connection->database->objectDeserializer(collection, key, data);
	}
	else if (status == SQLITE_ERROR)
	{
		YDBLogError(@"Error executing 'getDataForKeyStatement': %d %s, key(%@)",
                    status, sqlite3_errmsg(connection->db), key);
	}

	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
	FreeYapDatabaseString(&_key);

	if (object)
		[self.objectCache setObject:object forKey:cacheKey];

	return object;
}

- (id)metadataForKey:(NSString *)key inCollection:(NSString *)collection {
	return nil;
}

- (BOOL)getObject:(id *)objectPtr metadata:(id *)metadataPtr forCollectionKey:(YapCollectionKey *)collectionKey withRowid:(int64_t)rowid {
    id object = [self.objectCache objectForKey:collectionKey];
	id metadata = [self.metadataCache objectForKey:collectionKey];

	if (object || metadata)
	{
		if (object == nil)
		{
			object = [self objectForCollectionKey:collectionKey withRowid:rowid];
		}
		else if (metadata == nil)
		{
			metadata = [self metadataForCollectionKey:collectionKey withRowid:rowid];
		}

		if (objectPtr) *objectPtr = object;
		if (metadataPtr) *metadataPtr = metadata;
		return YES;
	}

	sqlite3_stmt *statement = [self getAllForRowidStatement];
	if (statement == NULL) {
		if (objectPtr) *objectPtr = nil;
		if (metadataPtr) *metadataPtr = nil;
		return NO;
	}

	// SELECT "data", "metadata" FROM "view-model-table" WHERE "rowid" = ?;

	sqlite3_bind_int64(statement, 1, rowid);

	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (connection->needsMarkSqlLevelSharedReadLock)
			[connection markSqlLevelSharedReadLockAcquired];

		const void *blob = sqlite3_column_blob(statement, 0);
		int blobSize = sqlite3_column_bytes(statement, 0);

		// Performance tuning:
		// Use dataWithBytesNoCopy to avoid an extra allocation and memcpy.

		NSData *data = [NSData dataWithBytesNoCopy:(void *)blob length:blobSize freeWhenDone:NO];
		object = connection->database->objectDeserializer(collectionKey.collection, collectionKey.key, data);

		if (object)
			[self.objectCache setObject:object forKey:collectionKey];

		const void *mBlob = sqlite3_column_blob(statement, 1);
		int mBlobSize = sqlite3_column_bytes(statement, 1);

		if (mBlobSize > 0)
		{
			// Performance tuning:
			// Use dataWithBytesNoCopy to avoid an extra allocation and memcpy.

			NSData *mData = [NSData dataWithBytesNoCopy:(void *)mBlob length:mBlobSize freeWhenDone:NO];
			metadata = connection->database->metadataDeserializer(collectionKey.collection, collectionKey.key, mData);
		}

		if (metadata)
			[self.metadataCache setObject:metadata forKey:collectionKey];
		else
			[self.metadataCache setObject:[YapNull null] forKey:collectionKey];
	}
	else if (status == SQLITE_ERROR)
	{
		YDBLogError(@"Error executing 'getKeyForRowidStatement': %d %s", status, sqlite3_errmsg(connection->db));
	}

	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);

	if (objectPtr) *objectPtr = object;
	if (metadataPtr) *metadataPtr = metadata;
	return YES;
}

- (BOOL)getObject:(id *)objectPtr metadata:(id *)metadataPtr forKey:(NSString *)key inCollection:(NSString *)collection {
    if (key == nil)
	{
		if (objectPtr) *objectPtr = nil;
		if (metadataPtr) *metadataPtr = nil;

		return NO;
	}
	if (collection == nil) collection = @"";

	YapCollectionKey *cacheKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];

	id object = [self.objectCache objectForKey:cacheKey];
	id metadata = [self.metadataCache objectForKey:cacheKey];

	BOOL found = NO;

	if (object && metadata)
	{
		// Both object and metadata were in cache.
		found = YES;

		// Need to check for empty metadata placeholder from cache.
		if (metadata == [YapNull null])
			metadata = nil;
	}
	else if (!object && metadata)
	{
		// Metadata was in cache.
		found = YES;

		// Need to check for empty metadata placeholder from cache.
		if (metadata == [YapNull null])
			metadata = nil;

		// Missing object. Fetch individually if requested.
		if (objectPtr)
			object = [self objectForKey:key inCollection:collection];
	}
	else if (object && !metadata)
	{
		// Object was in cache.
		found = YES;

		// Missing metadata. Fetch individually if requested.
		if (metadataPtr)
			metadata = [self metadataForKey:key inCollection:collection];
	}
	else // (!object && !metadata)
	{
		// Both object and metadata are missing.
		// Fetch via query.

		sqlite3_stmt *statement = [self getAllForKeyStatement];
		if (statement)
		{
			// SELECT "data", "metadata" FROM "view-model-table" WHERE "key" = ? ;

			YapDatabaseString _key; MakeYapDatabaseString(&_key, key);
			sqlite3_bind_text(statement, 1, _key.str, _key.length, SQLITE_STATIC);

			int status = sqlite3_step(statement);
			if (status == SQLITE_ROW)
			{
				if (connection->needsMarkSqlLevelSharedReadLock)
					[connection markSqlLevelSharedReadLockAcquired];

				const void *oBlob = sqlite3_column_blob(statement, 0);
				int oBlobSize = sqlite3_column_bytes(statement, 0);

				const void *mBlob = sqlite3_column_blob(statement, 1);
				int mBlobSize = sqlite3_column_bytes(statement, 1);

				if (objectPtr)
				{
					NSData *oData = [NSData dataWithBytesNoCopy:(void *)oBlob length:oBlobSize freeWhenDone:NO];
					object = connection->database->objectDeserializer(collection, key, oData);
				}

				if (metadataPtr && mBlobSize > 0)
				{
					NSData *mData = [NSData dataWithBytesNoCopy:(void *)mBlob length:mBlobSize freeWhenDone:NO];
					metadata = connection->database->metadataDeserializer(collection, key, mData);
				}

				found = YES;
			}
			else if (status == SQLITE_ERROR)
			{
				YDBLogError(@"Error executing 'getAllForKeyStatement': %d %s",
                            status, sqlite3_errmsg(connection->db));
			}

			if (object)
			{
				[self.objectCache setObject:object forKey:cacheKey];

				if (metadata)
					[self.metadataCache setObject:metadata forKey:cacheKey];
				else
					[self.metadataCache setObject:[YapNull null] forKey:cacheKey];
			}

			sqlite3_clear_bindings(statement);
			sqlite3_reset(statement);
			FreeYapDatabaseString(&_key);
		}
	}

	if (objectPtr) *objectPtr = object;
	if (metadataPtr) *metadataPtr = metadata;

	return found;
}

- (void)enumerateCollectionsUsingBlock:(void (^)(NSString *, BOOL *))block {
    if (block) {
        BOOL stop = YES;
        block(self.viewModelName, &stop);
    }
}

#pragma mark - Private header methods

- (void)_enumerateRowsInCollections:(NSArray *)collections usingBlock:(void (^)(int64_t, NSString *, NSString *, id, id, BOOL *))block withFilter:(BOOL (^)(int64_t, NSString *, NSString *))filter {
    if (block == NULL) return;
	NSString *collection = self.viewModelName;

	sqlite3_stmt *statement = [self enumerateRowsInCollectionStatement];
	if (statement == NULL) return;

	isMutated = NO; // mutation during enumeration protection
	BOOL stop = NO;

	BOOL unlimitedObjectCacheLimit = (connection->objectCacheLimit == 0);
	BOOL unlimitedMetadataCacheLimit = (connection->metadataCacheLimit == 0);

	// SELECT "rowid", "key", "data", "metadata" FROM "view-model-table";
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

    int status = sqlite3_step(statement);
    if (status == SQLITE_ROW)
    {
        if (connection->needsMarkSqlLevelSharedReadLock)
            [connection markSqlLevelSharedReadLockAcquired];

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

                id object = [self.objectCache objectForKey:cacheKey];
                if (object == nil)
                {
                    const void *oBlob = sqlite3_column_blob(statement, 2);
                    int oBlobSize = sqlite3_column_bytes(statement, 2);

                    NSData *oData = [NSData dataWithBytesNoCopy:(void *)oBlob length:oBlobSize freeWhenDone:NO];
                    object = connection->database->objectDeserializer(collection, key, oData);

                    if (unlimitedObjectCacheLimit ||
                        [self.objectCache count] < connection->objectCacheLimit)
                    {
                        if (object)
                            [self.objectCache setObject:object forKey:cacheKey];
                    }
                }

                id metadata = [self.metadataCache objectForKey:cacheKey];
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
                        metadata = connection->database->metadataDeserializer(collection, key, mData);
                    }

                    if (unlimitedMetadataCacheLimit ||
                        [self.metadataCache count] < connection->metadataCacheLimit)
                    {
                        if (metadata)
                            [self.metadataCache setObject:metadata forKey:cacheKey];
                        else
                            [self.metadataCache setObject:[YapNull null] forKey:cacheKey];
                    }
                }

                block(rowid, collection, key, object, metadata, &stop);

                if (stop || isMutated) break;
            }

        } while ((status = sqlite3_step(statement)) == SQLITE_ROW);
    }

    if ((status != SQLITE_DONE) && !stop && !isMutated)
    {
        YDBLogError(@"%@ - sqlite_step error: %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
    }

    sqlite3_clear_bindings(statement);
    sqlite3_reset(statement);

    if (isMutated && !stop)
    {
        @throw [self mutationDuringEnumerationException];
    }
}

- (void)_enumerateKeysInCollections:(NSArray *)collections usingBlock:(void (^)(int64_t, NSString *, NSString *, BOOL *))block {
    if (block == NULL) return;
    NSString *collection = self.viewModelName;

	sqlite3_stmt *statement = [self enumerateKeysInCollectionStatement];
	if (statement == NULL) return;

	isMutated = NO; // mutation during enumeration protection
	BOOL stop = NO;

	// SELECT "rowid", "key" FROM "view-model-table";
    int status = sqlite3_step(statement);
    if (status == SQLITE_ROW)
    {
        if (connection->needsMarkSqlLevelSharedReadLock)
            [connection markSqlLevelSharedReadLockAcquired];

        do
        {
            int64_t rowid = sqlite3_column_int64(statement, 0);

            const unsigned char *text = sqlite3_column_text(statement, 1);
            int textSize = sqlite3_column_bytes(statement, 1);

            NSString *key = [[NSString alloc] initWithBytes:text length:textSize encoding:NSUTF8StringEncoding];

            block(rowid, collection, key, &stop);

            if (stop || isMutated) break;

        } while ((status = sqlite3_step(statement)) == SQLITE_ROW);
    }

    if ((status != SQLITE_DONE) && !stop && !isMutated)
    {
        YDBLogError(@"%@ - sqlite_step error: %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
    }

    sqlite3_clear_bindings(statement);
    sqlite3_reset(statement);

    if (isMutated && !stop)
    {
        @throw [self mutationDuringEnumerationException];
    }
}

- (void)_enumerateKeysAndObjectsInCollections:(NSArray *)collections usingBlock:(void (^)(int64_t, NSString *, NSString *, id, BOOL *))block withFilter:(BOOL (^)(int64_t, NSString *, NSString *))filter {
    if (block == NULL) return;
    NSString *collection = self.viewModelName;

	sqlite3_stmt *statement = [self enumerateKeysAndObjectsInCollectionStatement];
	if (statement == NULL) return;

	isMutated = NO; // mutation during enumeration protection
	BOOL stop = NO;

	BOOL unlimitedObjectCacheLimit = (connection->objectCacheLimit == 0);

	// SELECT "rowid", "key", "data", FROM "view-model-table";
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

    int status = sqlite3_step(statement);
    if (status == SQLITE_ROW)
    {
        if (connection->needsMarkSqlLevelSharedReadLock)
            [connection markSqlLevelSharedReadLockAcquired];

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

                id object = [self.objectCache objectForKey:cacheKey];
                if (object == nil)
                {
                    const void *oBlob = sqlite3_column_blob(statement, 2);
                    int oBlobSize = sqlite3_column_bytes(statement, 2);

                    NSData *oData = [NSData dataWithBytesNoCopy:(void *)oBlob length:oBlobSize freeWhenDone:NO];
                    object = connection->database->objectDeserializer(collection, key, oData);

                    if (unlimitedObjectCacheLimit ||
                        [self.objectCache count] < connection->objectCacheLimit)
                    {
                        if (object)
                            [self.objectCache setObject:object forKey:cacheKey];
                    }
                }

                block(rowid, collection, key, object, &stop);

                if (stop || isMutated) break;
            }

        } while ((status = sqlite3_step(statement)) == SQLITE_ROW);
    }

    if ((status != SQLITE_DONE) && !stop && !isMutated)
    {
        YDBLogError(@"%@ - sqlite_step error: %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
    }

    sqlite3_clear_bindings(statement);
    sqlite3_reset(statement);

    if (isMutated && !stop)
    {
        @throw [self mutationDuringEnumerationException];
    }
}


- (void)_enumerateKeysAndMetadataInCollections:(NSArray *)collections usingBlock:(void (^)(int64_t, NSString *, NSString *, id, BOOL *))block withFilter:(BOOL (^)(int64_t, NSString *, NSString *))filter   {
    if (block == NULL) return;
	NSString *collection = self.viewModelName;

	sqlite3_stmt *statement = [self enumerateKeysAndMetadataInCollectionStatement];
	if (statement == NULL) return;

	isMutated = NO; // mutation during enumeration protection
	BOOL stop = NO;

	BOOL unlimitedMetadataCacheLimit = (connection->metadataCacheLimit == 0);

	// SELECT "rowid", "key", "metadata" FROM "view-model-table";
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

    int status = sqlite3_step(statement);
    if (status == SQLITE_ROW)
    {
        if (connection->needsMarkSqlLevelSharedReadLock)
            [connection markSqlLevelSharedReadLockAcquired];

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

                id metadata = [self.metadataCache objectForKey:cacheKey];
                if (metadata)
                {
                    if (metadata == [YapNull null])
                        metadata = nil;
                }
                else
                {
                    const void *mBlob = sqlite3_column_blob(statement, 2);
                    int mBlobSize = sqlite3_column_bytes(statement, 2);

                    if (mBlobSize > 0)
                    {
                        NSData *mData = [NSData dataWithBytesNoCopy:(void *)mBlob length:mBlobSize freeWhenDone:NO];
                        metadata = connection->database->metadataDeserializer(collection, key, mData);
                    }

                    if (unlimitedMetadataCacheLimit ||
                        [self.metadataCache count] < connection->metadataCacheLimit)
                    {
                        if (metadata)
                            [self.metadataCache setObject:metadata forKey:cacheKey];
                        else
                            [self.metadataCache setObject:[YapNull null] forKey:cacheKey];
                    }
                }

                block(rowid, collection, key, metadata, &stop);

                if (stop || isMutated) break;
            }

        } while ((status = sqlite3_step(statement)) == SQLITE_ROW);
    }

    if ((status != SQLITE_DONE) && !stop && !isMutated)
    {
        YDBLogError(@"%@ - sqlite_step error: %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
    }

    sqlite3_clear_bindings(statement);
    sqlite3_reset(statement);

    if (isMutated && !stop)
    {
        @throw [self mutationDuringEnumerationException];
    }
}

- (void)_enumerateRowsInAllCollectionsUsingBlock:(void (^)(int64_t, NSString *, NSString *, id, id, BOOL *))block withFilter:(BOOL (^)(int64_t, NSString *, NSString *))filter {
    if (block == NULL) return;

	sqlite3_stmt *statement = [self enumerateRowsInAllCollectionsStatement];
	if (statement == NULL) return;

	isMutated = NO; // mutation during enumeration protection
	BOOL stop = NO;

	// SELECT "rowid", "key", "data", "metadata" FROM "view-model-table" ";
	//           0       1       2        3

	BOOL unlimitedObjectCacheLimit = (connection->objectCacheLimit == 0);
	BOOL unlimitedMetadataCacheLimit = (connection->metadataCacheLimit == 0);

	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (connection->needsMarkSqlLevelSharedReadLock)
			[connection markSqlLevelSharedReadLockAcquired];

		do
		{
			int64_t rowid = sqlite3_column_int64(statement, 0);

			const unsigned char *text2 = sqlite3_column_text(statement, 1);
			int textSize2 = sqlite3_column_bytes(statement, 1);

			NSString *collection, *key;

			collection = self.viewModelName;
			key        = [[NSString alloc] initWithBytes:text2 length:textSize2 encoding:NSUTF8StringEncoding];

			BOOL invokeBlock = (filter == NULL) ? YES : filter(rowid, collection, key);
			if (invokeBlock)
			{
				YapCollectionKey *cacheKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];

				id object = [self.objectCache objectForKey:cacheKey];
				if (object == nil)
				{
					const void *oBlob = sqlite3_column_blob(statement, 2);
					int oBlobSize = sqlite3_column_bytes(statement, 2);

					NSData *oData = [NSData dataWithBytesNoCopy:(void *)oBlob length:oBlobSize freeWhenDone:NO];
					object = connection->database->objectDeserializer(collection, key, oData);

					if (unlimitedObjectCacheLimit || [self.objectCache count] < connection->objectCacheLimit)
					{
						if (object)
							[self.objectCache setObject:object forKey:cacheKey];
					}
				}

				id metadata = [self.metadataCache objectForKey:cacheKey];
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
						metadata = connection->database->metadataDeserializer(collection, key, mData);
					}

					if (unlimitedMetadataCacheLimit ||
					    [self.metadataCache count] < connection->metadataCacheLimit)
					{
						if (metadata)
							[self.metadataCache setObject:metadata forKey:cacheKey];
						else
							[self.metadataCache setObject:[YapNull null] forKey:cacheKey];
					}
				}

				block(rowid, collection, key, object, metadata, &stop);

				if (stop || isMutated) break;
			}

		} while ((status = sqlite3_step(statement)) == SQLITE_ROW);
	}

	if ((status != SQLITE_DONE) && !stop && !isMutated)
	{
		YDBLogError(@"%@ - sqlite_step error: %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
	}

	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);

	if (isMutated && !stop)
	{
		@throw [self mutationDuringEnumerationException];
	}
}

- (void)_enumerateKeysInAllCollectionsUsingBlock:(void (^)(int64_t, NSString *, NSString *, BOOL *))block {
    if (block == NULL) return;

	sqlite3_stmt *statement = [self enumerateKeysInAllCollectionsStatement];
	if (statement == NULL) return;

	isMutated = NO; // mutation during enumeration protection
	BOOL stop = NO;

	// SELECT "rowid", "key" FROM "view-model-table";

	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (connection->needsMarkSqlLevelSharedReadLock)
			[connection markSqlLevelSharedReadLockAcquired];

		do
		{
			int64_t rowid = sqlite3_column_int64(statement, 0);

			const unsigned char *text2 = sqlite3_column_text(statement, 1);
			int textSize2 = sqlite3_column_bytes(statement, 1);

			NSString *collection, *key;

			collection = self.viewModelName;
			key        = [[NSString alloc] initWithBytes:text2 length:textSize2 encoding:NSUTF8StringEncoding];

			block(rowid, collection, key, &stop);

			if (stop || isMutated) break;

		} while ((status = sqlite3_step(statement)) == SQLITE_ROW);
	}

	if ((status != SQLITE_DONE) && !stop && !isMutated)
	{
		YDBLogError(@"%@ - sqlite_step error: %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
	}

	sqlite3_reset(statement);

	if (isMutated && !stop)
	{
		@throw [self mutationDuringEnumerationException];
	}
}

- (void)_enumerateKeysAndObjectsInAllCollectionsUsingBlock:(void (^)(int64_t, NSString *, NSString *, id, BOOL *))block withFilter:(BOOL (^)(int64_t, NSString *, NSString *))filter {
    if (block == NULL) return;

	sqlite3_stmt *statement = [self enumerateKeysAndObjectsInAllCollectionsStatement];
	if (statement == NULL) return;

	isMutated = NO; // mutation during enumeration protection
	BOOL stop = NO;

	// SELECT "rowid", "key", "data" FROM "view-model-table";";
	//           0       1       2

	BOOL unlimitedObjectCacheLimit = (connection->objectCacheLimit == 0);

	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (connection->needsMarkSqlLevelSharedReadLock)
			[connection markSqlLevelSharedReadLockAcquired];

		do
		{
			int64_t rowid = sqlite3_column_int64(statement, 0);

			const unsigned char *text2 = sqlite3_column_text(statement, 1);
			int textSize2 = sqlite3_column_bytes(statement, 1);

			NSString *collection, *key;

			collection = self.viewModelName;
			key        = [[NSString alloc] initWithBytes:text2 length:textSize2 encoding:NSUTF8StringEncoding];

			BOOL invokeBlock = (filter == NULL) ? YES : filter(rowid, collection, key);
			if (invokeBlock)
			{
				YapCollectionKey *cacheKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];

				id object = [self.objectCache objectForKey:cacheKey];
				if (object == nil)
				{
					const void *oBlob = sqlite3_column_blob(statement, 2);
					int oBlobSize = sqlite3_column_bytes(statement, 2);

					NSData *oData = [NSData dataWithBytesNoCopy:(void *)oBlob length:oBlobSize freeWhenDone:NO];
					object = connection->database->objectDeserializer(collection, key, oData);

					if (unlimitedObjectCacheLimit || [self.objectCache count] < connection->objectCacheLimit)
					{
						if (object)
							[self.objectCache setObject:object forKey:cacheKey];
					}
				}

				block(rowid, collection, key, object, &stop);

				if (stop || isMutated) break;
			}

		} while ((status = sqlite3_step(statement)) == SQLITE_ROW);
	}

	if ((status != SQLITE_DONE) && !stop && !isMutated)
	{
		YDBLogError(@"%@ - sqlite_step error: %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
	}

	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);

	if (isMutated && !stop)
	{
		@throw [self mutationDuringEnumerationException];
	}

}

- (void)_enumerateKeysAndMetadataInAllCollectionsUsingBlock:(void (^)(int64_t, NSString *, NSString *, id, BOOL *))block withFilter:(BOOL (^)(int64_t, NSString *, NSString *))filter {
    if (block == NULL) return;

	sqlite3_stmt *statement = [self enumerateKeysAndMetadataInAllCollectionsStatement];
	if (statement == NULL) return;

	isMutated = NO; // mutation during enumeration protection
	BOOL stop = NO;

	// SELECT "rowid", "key", "metadata" FROM "view-model-table";
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

	BOOL unlimitedMetadataCacheLimit = (connection->metadataCacheLimit == 0);

	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (connection->needsMarkSqlLevelSharedReadLock)
			[connection markSqlLevelSharedReadLockAcquired];

		do
		{
			int64_t rowid = sqlite3_column_int64(statement, 0);

			const unsigned char *text2 = sqlite3_column_text(statement, 1);
			int textSize2 = sqlite3_column_bytes(statement, 1);

			NSString *collection, *key;

			collection = self.viewModelName;
			key        = [[NSString alloc] initWithBytes:text2 length:textSize2 encoding:NSUTF8StringEncoding];

			BOOL invokeBlock = (filter == NULL) ? YES : filter(rowid, collection, key);
			if (invokeBlock)
			{
				YapCollectionKey *cacheKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];

				id metadata = [self.metadataCache objectForKey:cacheKey];
				if (metadata)
				{
					if (metadata == [YapNull null])
						metadata = nil;
				}
				else
				{
					const void *mBlob = sqlite3_column_blob(statement, 2);
					int mBlobSize = sqlite3_column_bytes(statement, 2);

					if (mBlobSize > 0)
					{
						NSData *mData = [NSData dataWithBytesNoCopy:(void *)mBlob length:mBlobSize freeWhenDone:NO];
						metadata = connection->database->metadataDeserializer(collection, key, mData);
					}

					if (unlimitedMetadataCacheLimit ||
					    [self.metadataCache count] < connection->metadataCacheLimit)
					{
						if (metadata)
							[self.metadataCache setObject:metadata forKey:cacheKey];
						else
							[self.metadataCache setObject:[YapNull null] forKey:cacheKey];
					}
				}

				block(rowid, collection, key, metadata, &stop);

				if (stop || isMutated) break;
			}

		} while ((status = sqlite3_step(statement)) == SQLITE_ROW);
	}

	if ((status != SQLITE_DONE) && !stop && !isMutated)
	{
		YDBLogError(@"%@ - sqlite_step error: %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
	}

	sqlite3_reset(statement);

	if (isMutated && !stop)
	{
		@throw [self mutationDuringEnumerationException];
	}
}


#pragma mark - SQL methods

- (sqlite3_stmt *)getRowidForKeyStatement
{
	sqlite3_stmt **statement = &getRowidForKeyStatement;
	if (*statement == NULL)
	{
        NSString *q = [NSString stringWithFormat:@"SELECT \"rowid\" FROM \"%@\" WHERE \"key\" = ?;", self.viewModelTableName];
		const char *stmt = q.UTF8String;
		int stmtLen = (int)strlen(stmt);

		int status = sqlite3_prepare_v2(connection->db, stmt, stmtLen+1, statement, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"Error creating '%@': %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
		}
	}

	return *statement;
}

- (sqlite3_stmt *)enumerateRowsInCollectionStatement
{
	sqlite3_stmt **statement = &enumerateRowsInCollectionStatement;
	if (*statement == NULL)
	{
        NSString *q = [NSString stringWithFormat:@"SELECT \"rowid\", \"key\", \"data\", \"metadata\" FROM \"%@\";", self.viewModelTableName];

		const char *stmt = q.UTF8String;
		int stmtLen = (int)strlen(stmt);

		int status = sqlite3_prepare_v2(connection->db, stmt, stmtLen+1, statement, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"Error creating '%@': %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
		}
	}

	return *statement;
}

- (sqlite3_stmt *)getKeyForRowidStatement
{
	sqlite3_stmt **statement = &getKeyForRowidStatement;
	if (*statement == NULL)
	{
        NSString *q = [NSString stringWithFormat:@"SELECT \"key\" FROM \"%@\" WHERE \"rowid\" = ?;", self.viewModelTableName];
		const char *stmt = q.UTF8String;
		int stmtLen = (int)strlen(stmt);

		int status = sqlite3_prepare_v2(connection->db, stmt, stmtLen+1, statement, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"Error creating '%@': %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
		}
	}

	return *statement;
}

- (sqlite3_stmt *)getDataForRowidStatement
{
	sqlite3_stmt **statement = &getDataForRowidStatement;
	if (*statement == NULL)
	{
        NSString *q = [NSString stringWithFormat:@"SELECT \"data\" FROM \"%@\" WHERE \"rowid\" = ?;", self.viewModelTableName];
		const char *stmt = q.UTF8String;
		int stmtLen = (int)strlen(stmt);

		int status = sqlite3_prepare_v2(connection->db, stmt, stmtLen+1, statement, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"Error creating '%@': %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
		}
	}

	return *statement;
}

- (sqlite3_stmt *)getMetadataForRowidStatement
{
	sqlite3_stmt **statement = &getMetadataForRowidStatement;
	if (*statement == NULL)
	{
        NSString *q = [NSString stringWithFormat:@"SELECT \"metadata\" FROM \"%@\" WHERE \"rowid\" = ?;", self.viewModelTableName];
		const char *stmt = q.UTF8String;
		int stmtLen = (int)strlen(stmt);

		int status = sqlite3_prepare_v2(connection->db, stmt, stmtLen+1, statement, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"Error creating '%@': %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
		}
	}

	return *statement;
}

- (sqlite3_stmt *)getAllForRowidStatement
{
	sqlite3_stmt **statement = &getAllForRowidStatement;
	if (*statement == NULL)
	{
        NSString *q = [NSString stringWithFormat:@"SELECT \"data\", \"metadata\" FROM \"%@\" WHERE \"rowid\" = ?;", self.viewModelTableName];
		const char *stmt = q.UTF8String;
		int stmtLen = (int)strlen(stmt);

		int status = sqlite3_prepare_v2(connection->db, stmt, stmtLen+1, statement, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"Error creating '%@': %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
		}
	}

	return *statement;
}

- (sqlite3_stmt *)getAllForKeyStatement
{
	sqlite3_stmt **statement = &getAllForKeyStatement;
	if (*statement == NULL)
	{
        NSString *q = [NSString stringWithFormat:@"SELECT \"data\", \"metadata\" FROM \"%@\" WHERE \"key\" = ?;", self.viewModelTableName];
		const char *stmt = q.UTF8String;
		int stmtLen = (int)strlen(stmt);

		int status = sqlite3_prepare_v2(connection->db, stmt, stmtLen+1, statement, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"Error creating '%@': %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
		}
	}

	return *statement;
}

- (sqlite3_stmt *)getDataForKeyStatement
{
	sqlite3_stmt **statement = &getDataForKeyStatement;
	if (*statement == NULL)
	{
        NSString *q = [NSString stringWithFormat:@"SELECT \"data\" FROM \"%@\" WHERE \"key\" = ?;", self.viewModelTableName];
		const char *stmt = q.UTF8String;
		int stmtLen = (int)strlen(stmt);

		int status = sqlite3_prepare_v2(connection->db, stmt, stmtLen+1, statement, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"Error creating '%@': %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
		}
	}

	return *statement;
}

- (sqlite3_stmt *)enumerateKeysInCollectionStatement
{
	sqlite3_stmt **statement = &enumerateKeysInCollectionStatement;
	if (*statement == NULL)
	{
        NSString *q = [NSString stringWithFormat:@"SELECT \"rowid\", \"key\" FROM \"%@\";", self.viewModelTableName];
		const char *stmt = q.UTF8String;
		int stmtLen = (int)strlen(stmt);

		int status = sqlite3_prepare_v2(connection->db, stmt, stmtLen+1, statement, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"Error creating '%@': %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
		}
	}

	return *statement;
}

- (sqlite3_stmt *)enumerateKeysAndObjectsInCollectionStatement
{
	sqlite3_stmt **statement = &enumerateKeysAndObjectsInCollectionStatement;
	if (*statement == NULL)
	{
        NSString *q = [NSString stringWithFormat:@"SELECT \"rowid\", \"key\", \"data\" FROM \"%@\"", self.viewModelTableName];

		const char *stmt = q.UTF8String;
		int stmtLen = (int)strlen(stmt);

		int status = sqlite3_prepare_v2(connection->db, stmt, stmtLen+1, statement, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"Error creating '%@': %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
		}
	}

	return *statement;
}

- (sqlite3_stmt *)enumerateKeysAndMetadataInCollectionStatement
{
	sqlite3_stmt **statement = &enumerateKeysAndMetadataInCollectionStatement;
	if (*statement == NULL)
	{
        NSString *q = [NSString stringWithFormat:@"SELECT \"rowid\", \"key\", \"metadata\" FROM \"%@\"", self.viewModelTableName];

		const char *stmt = q.UTF8String;
		int stmtLen = (int)strlen(stmt);

		int status = sqlite3_prepare_v2(connection->db, stmt, stmtLen+1, statement, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"Error creating '%@': %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
		}
	}

	return *statement;
}

- (sqlite3_stmt *)enumerateRowsInAllCollectionsStatement
{
	sqlite3_stmt **statement = &enumerateRowsInAllCollectionsStatement;
	if (*statement == NULL)
	{
        NSString *q = [NSString stringWithFormat:@"SELECT \"rowid\", \"key\", \"data\", \"metadata\""
                       " FROM \"%@\"", self.viewModelTableName];

		const char *stmt = q.UTF8String;
		int stmtLen = (int)strlen(stmt);

		int status = sqlite3_prepare_v2(connection->db, stmt, stmtLen+1, statement, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"Error creating '%@': %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
		}
	}
    
	return *statement;
}

- (sqlite3_stmt *)enumerateKeysInAllCollectionsStatement
{
	sqlite3_stmt **statement = &enumerateKeysInAllCollectionsStatement;
	if (*statement == NULL)
	{
        NSString *q = [NSString stringWithFormat:@"SELECT \"rowid\", \"key\" FROM \"%@\";", self.viewModelTableName];
        
		const char *stmt = q.UTF8String;
		int stmtLen = (int)strlen(stmt);
        
		int status = sqlite3_prepare_v2(connection->db, stmt, stmtLen+1, statement, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"Error creating '%@': %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
		}
	}
    
	return *statement;
}

- (sqlite3_stmt *)enumerateKeysAndObjectsInAllCollectionsStatement
{
	sqlite3_stmt **statement = &enumerateKeysAndObjectsInAllCollectionsStatement;
	if (*statement == NULL)
	{
        NSString *q = [NSString stringWithFormat:@"SELECT \"rowid\", \"key\", \"data\""
                       " FROM \"%@\";", self.viewModelTableName];
        
		const char *stmt = q.UTF8String;
		int stmtLen = (int)strlen(stmt);
        
		int status = sqlite3_prepare_v2(connection->db, stmt, stmtLen+1, statement, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"Error creating '%@': %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
		}
	}
    
	return *statement;
}

- (sqlite3_stmt *)enumerateKeysAndMetadataInAllCollectionsStatement
{
	sqlite3_stmt **statement = &enumerateKeysAndMetadataInAllCollectionsStatement;
	if (*statement == NULL)
	{
        NSString *q = [NSString stringWithFormat:@"SELECT \"rowid\", \"key\", \"metadata\""
                       " FROM \"%@\";", self.viewModelName];
        
		const char *stmt = q.UTF8String;
		int stmtLen = (int)strlen(stmt);
        
		int status = sqlite3_prepare_v2(connection->db, stmt, stmtLen+1, statement, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"Error creating '%@': %d %s", THIS_METHOD, status, sqlite3_errmsg(connection->db));
		}
	}
    
	return *statement;
}

@end
