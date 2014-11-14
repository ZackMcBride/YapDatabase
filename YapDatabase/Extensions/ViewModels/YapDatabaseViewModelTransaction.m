#import "YapDatabaseViewModelTransaction.h"
#import "YapDatabaseViewModelPrivate.h"

#import "YapDatabaseExtensionPrivate.h"
#import "YapDatabasePrivate.h"
#import "YapDatabaseStatement.h"
#import "YapWhiteListBlacklist.h"
#import "YapDatabaseQuery.h"

#import "YapDatabaseLogging.h"

#if ! __has_feature(objc_arc)
#warning This file must be compiled with ARC. Use -fobjc-arc flag (or convert project to ARC).
#endif

#if DEBUG
static const int ydbLogLevel = YDB_LOG_LEVEL_WARN;
#else
static const int ydbLogLevel = YDB_LOG_LEVEL_WARN;
#endif

static NSString *const ExtKey_classVersion       = @"classVersion";
static NSString *const ExtKey_versionTag         = @"versionTag";
static NSString *const ExtKey_version_deprecated = @"version";

@implementation YapDatabaseViewModelTransaction

- (instancetype)initWithViewModelConnection:(YapDatabaseViewModelConnection *)inViewModelConnection
                        databaseTransaction:(YapDatabaseReadTransaction *)inDatabaseTransaction
{
    self = [super init];
    if (self) {
        viewModelConnection = inViewModelConnection;
        databaseTransaction = inDatabaseTransaction;
    }
    return self;
}

- (BOOL)createIfNeeded
{
	int oldClassVersion = 0;
	BOOL hasOldClassVersion = [self getIntValue:&oldClassVersion forExtensionKey:ExtKey_classVersion persistent:YES];

	int classVersion = YAP_DATABASE_VIEW_MODEL_CLASS_VERSION;

	if (oldClassVersion != classVersion)
	{
		// First time registration (or at least for this version)

		if (hasOldClassVersion) {
			if (![self dropTable]) return NO;
		}

		if (![self createTable]) return NO;
		if (![self populate]) return NO;

		[self setIntValue:classVersion forExtensionKey:ExtKey_classVersion persistent:YES];

		NSString *versionTag = viewModelConnection->viewModel->versionTag;
		[self setStringValue:versionTag forExtensionKey:ExtKey_versionTag persistent:YES];
	} else {
		// Check user-supplied versionTag.
		// We may need to re-populate the database if it changed.

		NSString *versionTag = viewModelConnection->viewModel->versionTag;

		NSString *oldVersionTag = [self stringValueForExtensionKey:ExtKey_versionTag persistent:YES];

		BOOL hasOldVersion_deprecated = NO;
		if (oldVersionTag == nil)
		{
			int oldVersion_deprecated = 0;
			hasOldVersion_deprecated = [self getIntValue:&oldVersion_deprecated
			                             forExtensionKey:ExtKey_version_deprecated persistent:YES];

			if (hasOldVersion_deprecated)
			{
				oldVersionTag = [NSString stringWithFormat:@"%d", oldVersion_deprecated];
			}
		}

		if (![oldVersionTag isEqualToString:versionTag])
		{
			if (![self dropTable]) return NO;
			if (![self createTable]) return NO;
			if (![self populate]) return NO;

			[self setStringValue:versionTag forExtensionKey:ExtKey_versionTag persistent:YES];

			if (hasOldVersion_deprecated)
				[self removeValueForExtensionKey:ExtKey_version_deprecated persistent:YES];
		}
		else if (hasOldVersion_deprecated)
		{
			[self removeValueForExtensionKey:ExtKey_version_deprecated persistent:YES];
			[self setStringValue:versionTag forExtensionKey:ExtKey_versionTag persistent:YES];
		}

		// The following code is designed to assist developers in understanding extension changes.
		// The rules are straight-forward and easy to remember:
		//
		// - If you make ANY changes to the configuration of the extension then you MUST change the version.
		//
		// For this extension, that means you MUST change the version if ANY of the following are true:
		//
		// - you changed the setup
		// - you changed the block in any meaningful way (which would result in different values for any existing row)
		//
		// Note: The code below detects only changes to the setup.
		// It could theoretically handle such changes, and automatically force a repopulation.
		// This is a bad idea for two reasons:
		//
		// - First, it complicates the rules. The rules, as stated above, are simple. They follow the KISS principle.
		//   Changing these rules would pose a complication that increases cognitive overhead.
		//   It may be easy to remember now, but 6 months from now the nuance has become hazy.
		//   Additionally, the rest of the database system follows the same set of rules.
		//   So adding a complication for just a particular extension is even more confusing.
		//
		// - Second, it adds overhead to the registration process.
		//   This sanity check doesn't come for free.
		//   And the overhead is only helpful during the development lifecycle.
		//   It's certainly not something you want in a shipped version.
		//
#if DEBUG
		if ([oldVersionTag isEqualToString:versionTag])
		{
			sqlite3 *db = databaseTransaction->connection->db;

			NSDictionary *columns = [YapDatabase columnNamesAndAffinityForTable:[self tableName] using:db];
		}
#endif
	}

	return YES;
}

- (BOOL)prepareIfNeeded
{
	return YES;
}

- (BOOL)dropTable
{
	sqlite3 *db = databaseTransaction->connection->db;

	NSString *tableName = [self tableName];
	NSString *dropTable = [NSString stringWithFormat:@"DROP TABLE IF EXISTS \"%@\";", tableName];

	int status = sqlite3_exec(db, [dropTable UTF8String], NULL, NULL, NULL);
	if (status != SQLITE_OK)
	{
		YDBLogError(@"%@ - Failed dropping view model table (%@): %d %s",
		            THIS_METHOD, dropTable, status, sqlite3_errmsg(db));
		return NO;
	}

	return YES;
}

- (BOOL)createTable
{
	sqlite3 *db = databaseTransaction->connection->db;

	NSString *tableName = [self tableName];

	YDBLogVerbose(@"Creating view model table for registeredName(%@): %@", [self registeredName], tableName);

	// CREATE TABLE  IF NOT EXISTS "tableName" ("rowid" INTEGER PRIMARY KEY, index1, index2...);

	NSMutableString *createTable = [NSMutableString stringWithCapacity:100];
	[createTable appendFormat:@"CREATE TABLE IF NOT EXISTS \"%@\" (\"rowid\" INTEGER PRIMARY KEY, \"key\" TEXT, \"data\" BLOB);", tableName];

	int status = sqlite3_exec(db, [createTable UTF8String], NULL, NULL, NULL);
	if (status != SQLITE_OK)
	{
		YDBLogError(@"%@ - Failed creating view model table (%@): %d %s",
		            THIS_METHOD, tableName, status, sqlite3_errmsg(db));
		return NO;
	}

    NSMutableString *createIndex =
    [NSMutableString stringWithFormat:@"CREATE INDEX IF NOT EXISTS \"rowid\" ON \"%@\" (\"rowid\");", tableName];

    [createIndex appendFormat:@"CREATE INDEX IF NOT EXISTS \"key\" ON \"%@\" (\"key\");", tableName];

    status = sqlite3_exec(db, [createIndex UTF8String], NULL, NULL, NULL);
    if (status != SQLITE_OK)
    {
        YDBLogError(@"Failed creating index on '%@': %d %s", tableName, status, sqlite3_errmsg(db));
        return NO;
    }

	return YES;
}

- (BOOL)populate
{
	// Remove everything from the database

	[self removeAllRowids];

	// Enumerate the existing rows in the database and populate the indexes

	__unsafe_unretained YapDatabaseViewModel *viewModel = viewModelConnection->viewModel;

	__unsafe_unretained YapWhitelistBlacklist *allowedCollections = viewModel->options.allowedCollections;

    __unsafe_unretained YapDatabaseViewModelWithObjectBlock viewModelBlock =
    (YapDatabaseViewModelWithObjectBlock)viewModel->block;


    void (^enumBlock)(int64_t rowid, NSString *collection, NSString *key, id object, id metadata, BOOL *stop);
    enumBlock = ^(int64_t rowid, NSString *collection, NSString *key, id object, id metadata, BOOL *stop) {
        [self insertOrUpdateViewModelOnObjectChange:object forCollectionKey:collection withMetadata:metadata rowid:rowid];
    };

    if (allowedCollections)
    {
        [databaseTransaction enumerateCollectionsUsingBlock:^(NSString *collection, BOOL *stop) {

            if ([allowedCollections isAllowed:collection])
            {
                [databaseTransaction _enumerateRowsInCollection:@[ collection ]
                                                     usingBlock:
                 ^(int64_t rowid, NSString *key, id object, id metadata, BOOL *stop) {
                     enumBlock(rowid, collection, key, object, metadata, stop);
                }];
            }
        }];
    }
    else
    {
        [databaseTransaction _enumerateRowsInAllCollectionsUsingBlock:enumBlock];
    }

	return YES;
}

/**
 * Required override method from YapDatabaseExtensionTransaction.
 **/
- (YapDatabaseReadTransaction *)databaseTransaction
{
	return databaseTransaction;
}

/**
 * Required override method from YapDatabaseExtensionTransaction.
 **/
- (YapDatabaseExtensionConnection *)extensionConnection
{
	return viewModelConnection;
}

- (NSString *)registeredName
{
	return [viewModelConnection->viewModel registeredName];
}

- (NSString *)tableName
{
	return [viewModelConnection->viewModel tableName];
}

- (void)addViewModelObject:(id)object withPrimaryKey:(NSString *)primarykey rowId:(int64_t)rowid
{
	YDBLogAutoTrace();

	sqlite3_stmt *statement = [viewModelConnection insertStatement];

	if (statement == NULL)
		return;

    YapDatabase *database = viewModelConnection->databaseConnection->database;
    if (database->objectSanitizer)
	{
		object = database->objectSanitizer(nil, primarykey, object);
		if (object == nil)
		{
			YDBLogWarn(@"Object sanitizer returned nil for key(%@) object: %@", primarykey, object);
			return;
		}
	}

    // To use SQLITE_STATIC on our data, we use the objc_precise_lifetime attribute.
	// This ensures the data isn't released until it goes out of scope.
    __attribute__((objc_precise_lifetime)) NSData *serializedObject = nil;
    serializedObject = database->objectSerializer(nil, primarykey, object);

	sqlite3_bind_int64(statement, 1, rowid);
    sqlite3_bind_text(statement, 2, [primarykey UTF8String], -1, SQLITE_TRANSIENT);
    sqlite3_bind_blob(statement, 3, serializedObject.bytes, (int)serializedObject.length, SQLITE_STATIC);

	int status = sqlite3_step(statement);
	if (status != SQLITE_DONE)
	{
		YDBLogError(@"Error executing 'insertStatement': %d %s",
		            status, sqlite3_errmsg(databaseTransaction->connection->db));
	}
    
	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);
    
	isMutated = YES;
}

- (void)removeRowid:(int64_t)rowid
{
	YDBLogAutoTrace();

	sqlite3_stmt *statement = [viewModelConnection removeStatement];
	if (statement == NULL) return;

	// DELETE FROM "tableName" WHERE "rowid" = ?;

	sqlite3_bind_int64(statement, 1, rowid);

	int status = sqlite3_step(statement);
	if (status != SQLITE_DONE)
	{
		YDBLogError(@"Error executing 'removeStatement': %d %s",
		            status, sqlite3_errmsg(databaseTransaction->connection->db));
	}

	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);

	isMutated = YES;
}

- (void)removeRowids:(NSArray *)rowids
{
	YDBLogAutoTrace();

	NSUInteger count = [rowids count];

	if (count == 0) return;
	if (count == 1)
	{
		int64_t rowid = [[rowids objectAtIndex:0] longLongValue];

		[self removeRowid:rowid];
		return;
	}

	// DELETE FROM "tableName" WHERE "rowid" in (?, ?, ...);
	//
	// Note: We don't have to worry sqlite's max number of host parameters.
	// YapDatabase gives us the rowids in batches where each batch is already capped at this number.

	NSUInteger capacity = 50 + (count * 3);
	NSMutableString *query = [NSMutableString stringWithCapacity:capacity];

	[query appendFormat:@"DELETE FROM \"%@\" WHERE \"rowid\" IN (", [self tableName]];

	NSUInteger i;
	for (i = 0; i < count; i++)
	{
		if (i == 0)
			[query appendFormat:@"?"];
		else
			[query appendFormat:@", ?"];
	}

	[query appendString:@");"];

	sqlite3_stmt *statement;

	int status = sqlite3_prepare_v2(databaseTransaction->connection->db, [query UTF8String], -1, &statement, NULL);
	if (status != SQLITE_OK)
	{
		YDBLogError(@"Error creating 'removeRowids' statement: %d %s",
		            status, sqlite3_errmsg(databaseTransaction->connection->db));
		return;
	}

	for (i = 0; i < count; i++)
	{
		int64_t rowid = [[rowids objectAtIndex:i] longLongValue];

		sqlite3_bind_int64(statement, (int)(i + 1), rowid);
	}

	status = sqlite3_step(statement);
	if (status != SQLITE_DONE)
	{
		YDBLogError(@"Error executing 'removeRowids' statement: %d %s",
		            status, sqlite3_errmsg(databaseTransaction->connection->db));
	}

	sqlite3_finalize(statement);

	isMutated = YES;
}

- (void)removeAllRowids
{
	YDBLogAutoTrace();

	sqlite3_stmt *statement = [viewModelConnection removeAllStatement];
	if (statement == NULL)
		return;

	int status;

	// DELETE FROM "tableName";

	YDBLogVerbose(@"DELETE FROM '%@';", [self tableName]);

	status = sqlite3_step(statement);
	if (status != SQLITE_DONE)
	{
		YDBLogError(@"%@ (%@): Error in removeAllStatement: %d %s",
		            THIS_METHOD, [self registeredName],
		            status, sqlite3_errmsg(databaseTransaction->connection->db));
	}

	sqlite3_reset(statement);

	isMutated = YES;
}

- (void)commitTransaction
{
	viewModelConnection = nil;
	databaseTransaction = nil;
}

- (void)rollbackTransaction
{
	viewModelConnection = nil;
	databaseTransaction = nil;
}

- (void)insertOrUpdateViewModelOnObjectChange:(id)object
                             forCollectionKey:(YapCollectionKey *)collectionKey
                                 withMetadata:(id)metadata
                                        rowid:(int64_t)rowid
{
	__unsafe_unretained YapDatabaseViewModel *viewModel = viewModelConnection->viewModel;

	__unsafe_unretained NSString *collection = collectionKey.collection;
	__unsafe_unretained NSString *key = collectionKey.key;

    __unsafe_unretained YapWhitelistBlacklist *allowedCollections = viewModel->options.allowedCollections;
	if (allowedCollections && ![allowedCollections isAllowed:collection])
	{
		return;
	}

    BOOL shouldProcessInsert = [viewModel->setup.relatedCollections containsObject:collection];

    if (shouldProcessInsert)
    {
        NSString *viewModelPrimaryKey = viewModel->setup.primaryKeyForObjectInCollection(object, collection);
        id currentViewModelObject = [self viewModelObjectForPrimaryKey:viewModelPrimaryKey];

        if (viewModel->blockType == YapDatabaseViewModelBlockTypeWithKey)
        {
            __unsafe_unretained YapDatabaseViewModelWithKeyBlock block =
            (YapDatabaseViewModelWithKeyBlock)viewModel->block;

            block(currentViewModelObject, collection, key, self);
        }
        else if (viewModel->blockType == YapDatabaseViewModelBlockTypeWithObject)
        {
            __unsafe_unretained YapDatabaseViewModelWithObjectBlock block =
            (YapDatabaseViewModelWithObjectBlock)viewModel->block;

            block(currentViewModelObject, collection, key, object, self);
        }
        else if (viewModel->blockType == YapDatabaseViewModelBlockTypeWithMetadata)
        {
            __unsafe_unretained YapDatabaseViewModelWithMetadataBlock block =
            (YapDatabaseViewModelWithMetadataBlock)viewModel->block;

            block(currentViewModelObject, collection, key, metadata, self);
        }
        else
        {
            __unsafe_unretained YapDatabaseViewModelWithRowBlock block =
            (YapDatabaseViewModelWithRowBlock)viewModel->block;

            block(currentViewModelObject, collection, key, object, metadata, self);
        }

        int64_t existingRowId = [self rowIdForRowWithPrimaryKey:viewModelPrimaryKey];
        if (existingRowId != -1) {
            rowid = existingRowId;
        }

        [self addViewModelObject:currentViewModelObject withPrimaryKey:viewModelPrimaryKey rowId:rowid];
    }
}

- (void)handleInsertObject:(id)object
          forCollectionKey:(YapCollectionKey *)collectionKey
              withMetadata:(id)metadata
                     rowid:(int64_t)rowid
{
    YDBLogAutoTrace();
    [self insertOrUpdateViewModelOnObjectChange:object forCollectionKey:collectionKey withMetadata:metadata rowid:rowid];
}

/**
 * YapDatabase extension hook.
 * This method is invoked by a YapDatabaseReadWriteTransaction as a post-operation-hook.
 **/
- (void)handleUpdateObject:(id)object
          forCollectionKey:(YapCollectionKey *)collectionKey
              withMetadata:(id)metadata
                     rowid:(int64_t)rowid
{
	YDBLogAutoTrace();
    [self insertOrUpdateViewModelOnObjectChange:object forCollectionKey:collectionKey withMetadata:metadata rowid:rowid];
}

/**
 * YapDatabase extension hook.
 * This method is invoked by a YapDatabaseReadWriteTransaction as a post-operation-hook.
 **/
- (void)handleReplaceObject:(id)object forCollectionKey:(YapCollectionKey *)collectionKey withRowid:(int64_t)rowid
{
	YDBLogAutoTrace();

	__unsafe_unretained YapDatabaseViewModel *viewModel = viewModelConnection->viewModel;

	__unsafe_unretained NSString *collection = collectionKey.collection;
	__unsafe_unretained NSString *key = collectionKey.key;

	__unsafe_unretained YapWhitelistBlacklist *allowedCollections = viewModel->options.allowedCollections;

    NSArray *mappedPrimaryKeyTuple;

	if (allowedCollections && ![allowedCollections isAllowed:collection])
	{
		return;
	}

	// Invoke the block to find out if the object should be included in the index.

	id metadata = nil;

	if (viewModel->blockType == YapDatabaseViewModelBlockTypeWithKey ||
	    viewModel->blockType == YapDatabaseViewModelBlockTypeWithMetadata)
	{
		// Index values are based on the key or object.
		// Neither have changed, and thus the values haven't changed.

		return;
	}
	else
	{
		// Index values are based on object or row (object+metadata).
		// Invoke block to see what the new values are.

        [self insertOrUpdateViewModelOnObjectChange:object forCollectionKey:collectionKey withMetadata:metadata rowid:rowid];
	}
}

/**
 * YapDatabase extension hook.
 * This method is invoked by a YapDatabaseReadWriteTransaction as a post-operation-hook.
 **/
- (void)handleReplaceMetadata:(id)metadata forCollectionKey:(YapCollectionKey *)collectionKey withRowid:(int64_t)rowid
{
	YDBLogAutoTrace();

	__unsafe_unretained YapDatabaseViewModel *ViewModel = viewModelConnection->viewModel;

	id object = nil;

	if (ViewModel->blockType == YapDatabaseViewModelBlockTypeWithKey ||
	    ViewModel->blockType == YapDatabaseViewModelBlockTypeWithObject)
	{
		// Index values are based on the key or object.
		// Neither have changed, and thus the values haven't changed.

		return;
	}
	else
	{
		// Index values are based on metadata or objectAndMetadata.
		// Invoke block to see what the new values are.

        [self insertOrUpdateViewModelOnObjectChange:object forCollectionKey:collectionKey withMetadata:metadata rowid:rowid];
	}
}

/**
 * YapDatabase extension hook.
 * This method is invoked by a YapDatabaseReadWriteTransaction as a post-operation-hook.
 **/
- (void)handleTouchObjectForCollectionKey:(YapCollectionKey *)collectionKey withRowid:(int64_t)rowid
{
	// Nothing to do for this extension
}

/**
 * YapDatabase extension hook.
 * This method is invoked by a YapDatabaseReadWriteTransaction as a post-operation-hook.
 **/
- (void)handleTouchMetadataForCollectionKey:(YapCollectionKey *)collectionKey withRowid:(int64_t)rowid
{
	// Nothing to do for this extension
}

/**
 * YapDatabase extension hook.
 * This method is invoked by a YapDatabaseReadWriteTransaction as a post-operation-hook.
 **/
- (void)handleRemoveObjectForCollectionKey:(YapCollectionKey *)collectionKey withRowid:(int64_t)rowid
{
	YDBLogAutoTrace();

	__unsafe_unretained YapDatabaseViewModel *viewModel = viewModelConnection->viewModel;

	__unsafe_unretained YapWhitelistBlacklist *allowedCollections = viewModel->options.allowedCollections;
	if (allowedCollections && ![allowedCollections isAllowed:collectionKey.collection])
	{
		return;
	}

	[self removeRowid:rowid];
}

/**
 * YapDatabase extension hook.
 * This method is invoked by a YapDatabaseReadWriteTransaction as a post-operation-hook.
 **/
- (void)handleRemoveObjectsForKeys:(NSArray *)keys inCollection:(NSString *)collection withRowids:(NSArray *)rowids
{
	YDBLogAutoTrace();

	__unsafe_unretained YapDatabaseViewModel *viewModel = viewModelConnection->viewModel;

	__unsafe_unretained YapWhitelistBlacklist *allowedCollections = viewModel->options.allowedCollections;
	if (allowedCollections && ![allowedCollections isAllowed:collection])
	{
		return;
	}

	[self removeRowids:rowids];
}

/**
 * YapDatabase extension hook.
 * This method is invoked by a YapDatabaseReadWriteTransaction as a post-operation-hook.
 **/
- (void)handleRemoveAllObjectsInAllCollections
{
	YDBLogAutoTrace();

	[self removeAllRowids];
}

- (int64_t)rowIdForRowWithPrimaryKey:(NSString *)primaryKey
{
    YapDatabaseQuery *query = [YapDatabaseQuery queryWithFormat:@"WHERE key=?", primaryKey];
    __block int64_t rowId = -1;
    [self _enumerateRowidsMatchingQuery:query usingBlock:^(int64_t existingRowId, BOOL *stop) {
        rowId = existingRowId;
        *stop = YES;
    }];
    return rowId;
}

- (id)viewModelObjectForPrimaryKey:(NSString *)primaryKey {
    YapDatabaseQuery *query = [YapDatabaseQuery queryWithFormat:@"WHERE key=?", primaryKey];

    __block id object;
    [self _enumerateRowidsMatchingQuery:query usingBlock:^(int64_t rowid, BOOL *stop) {
		object = [databaseTransaction objectForKey:primaryKey inCollection:nil withRowid:rowid];
        *stop = YES;
    }];
    return object;
}

- (BOOL)enumerateKeysAndObjectsMatchingQuery:(YapDatabaseQuery *)query
                                  usingBlock:
(void (^)(NSString *key, id object, BOOL *stop))block
{
	if (query == nil) return NO;
	if (block == nil) return NO;

	BOOL result = [self _enumerateRowidsMatchingQuery:query usingBlock:^(int64_t rowid, BOOL *stop) {

	}];

	return result;
}

- (BOOL)_enumerateRowidsMatchingQuery:(YapDatabaseQuery *)query
                           usingBlock:(void (^)(int64_t rowid, BOOL *stop))block
{
	// Create full query using given filtering clause(s)

	NSString *fullQueryString =
    [NSString stringWithFormat:@"SELECT \"rowid\" FROM \"%@\" %@;", [self tableName], query.queryString];

	// Turn query into compiled sqlite statement.
	// Use cache if possible.

	sqlite3_stmt *statement = NULL;

	YapDatabaseStatement *wrapper = [viewModelConnection->queryCache objectForKey:fullQueryString];
	if (wrapper)
	{
		statement = wrapper.stmt;
	}
	else
	{
		sqlite3 *db = databaseTransaction->connection->db;

		int status = sqlite3_prepare_v2(db, [fullQueryString UTF8String], -1, &statement, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"%@: Error creating query:\n query: '%@'\n error: %d %s",
						THIS_METHOD, fullQueryString, status, sqlite3_errmsg(db));

			return NO;
		}

		if (viewModelConnection->queryCache)
		{
			wrapper = [[YapDatabaseStatement alloc] initWithStatement:statement];
			[viewModelConnection->queryCache setObject:wrapper forKey:fullQueryString];
		}
	}

	// Bind query parameters appropriately.

	int i = 1;
	for (id value in query.queryParameters)
	{
		if ([value isKindOfClass:[NSNumber class]])
		{
			__unsafe_unretained NSNumber *cast = (NSNumber *)value;

			CFNumberType numType = CFNumberGetType((__bridge CFNumberRef)cast);

			if (numType == kCFNumberFloatType   ||
			    numType == kCFNumberFloat32Type ||
			    numType == kCFNumberFloat64Type ||
			    numType == kCFNumberDoubleType  ||
			    numType == kCFNumberCGFloatType  )
			{
				double num = [cast doubleValue];
				sqlite3_bind_double(statement, i, num);
			}
			else
			{
				int64_t num = [cast longLongValue];
				sqlite3_bind_int64(statement, i, (sqlite3_int64)num);
			}
		}
		else if ([value isKindOfClass:[NSDate class]])
		{
			__unsafe_unretained NSDate *cast = (NSDate *)value;

			double num = [cast timeIntervalSinceReferenceDate];
			sqlite3_bind_double(statement, i, num);
		}
		else if ([value isKindOfClass:[NSString class]])
		{
			__unsafe_unretained NSString *cast = (NSString *)value;

			sqlite3_bind_text(statement, i, [cast UTF8String], -1, SQLITE_TRANSIENT);
		}
		else
		{
			YDBLogWarn(@"Unable to bind value for with unsupported class: %@", NSStringFromClass([value class]));
		}

		i++;
	}

	// Enumerate query results

	BOOL stop = NO;
	isMutated = NO; // mutation during enumeration protection

	int status = sqlite3_step(statement);
	if (status == SQLITE_ROW)
	{
		if (databaseTransaction->connection->needsMarkSqlLevelSharedReadLock)
			[databaseTransaction->connection markSqlLevelSharedReadLockAcquired];

		do
		{
			int64_t rowid = sqlite3_column_int64(statement, 0);

			block(rowid, &stop);

			if (stop || isMutated) break;

		} while ((status = sqlite3_step(statement)) == SQLITE_ROW);
	}

	if ((status != SQLITE_DONE) && !stop && !isMutated)
	{
		YDBLogError(@"%@ - sqlite_step error: %d %s", THIS_METHOD,
		            status, sqlite3_errmsg(databaseTransaction->connection->db));
	}

	sqlite3_clear_bindings(statement);
	sqlite3_reset(statement);

	if (isMutated && !stop)
	{
		@throw [self mutationDuringEnumerationException];
	}

	return (status == SQLITE_DONE);
}

#pragma mark Exceptions

- (NSException *)mutationDuringEnumerationException
{
	NSString *reason = [NSString stringWithFormat:
                        @"ViewModel <RegisteredName=%@> was mutated while being enumerated.", [self registeredName]];

	NSDictionary *userInfo = @{ NSLocalizedRecoverySuggestionErrorKey:
                                    @"In general, you cannot modify the database while enumerating it."
                                @" This is similar in concept to an NSMutableArray."
                                @" If you only need to make a single modification, you may do so but you MUST set the 'stop' parameter"
                                @" of the enumeration block to YES (*stop = YES;) immediately after making the modification."};

	return [NSException exceptionWithName:@"YapDatabaseException" reason:reason userInfo:userInfo];
}

@end
