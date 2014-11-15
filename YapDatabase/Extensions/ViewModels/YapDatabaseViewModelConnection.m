#import "YapDatabaseViewModelConnection.h"

#import "YapDatabasePrivate.h"
#import "YapDatabaseExtensionPrivate.h"

#import "YapDatabaseViewModelPrivate.h"

#import "YapDatabaseLogging.h"

#if DEBUG
static const int ydbLogLevel = YDB_LOG_LEVEL_WARN;
#else
static const int ydbLogLevel = YDB_LOG_LEVEL_WARN;
#endif

@implementation YapDatabaseViewModelConnection
{
    sqlite3_stmt *getDataForRowidStatement;
	sqlite3_stmt *insertStatement;
	sqlite3_stmt *removeStatement;
	sqlite3_stmt *removeAllStatement;
}

#pragma mark - Object Lifecycle

@synthesize viewModel = viewModel;

- (instancetype)initWithViewModel:(YapDatabaseViewModel *)inViewModel
               databaseConnection:(YapDatabaseConnection *)inDatabaseConnection
{
    self = [super init];
    if (self) {
        viewModel = inViewModel;
        databaseConnection = inDatabaseConnection;

        queryCacheLimit = 10;
        queryCache = [[YapCache alloc] initWithKeyClass:[NSString class]
                                              countLimit:queryCacheLimit];
    }
    return self;
}

- (void)dealloc
{
	[queryCache removeAllObjects];
	[self _flushStatements];
}

- (void)_flushStatements
{
    sqlite_finalize_null(&getDataForRowidStatement);
	sqlite_finalize_null(&insertStatement);
	sqlite_finalize_null(&removeStatement);
	sqlite_finalize_null(&removeAllStatement);
}

- (void)_flushMemoryWithFlags:(YapDatabaseConnectionFlushMemoryFlags)flags
{
	if (flags & YapDatabaseConnectionFlushMemoryFlags_Caches)
	{
		[queryCache removeAllObjects];
	}

	if (flags & YapDatabaseConnectionFlushMemoryFlags_Statements)
	{
		[self _flushStatements];
	}
}

#pragma mark - Accessors

- (YapDatabaseExtension *)extension
{
	return viewModel;
}

#pragma mark - Configuration

- (BOOL)queryCacheEnabled
{
	__block BOOL result = NO;

	dispatch_block_t block = ^{

		result = (queryCache == nil) ? NO : YES;
	};

	if (dispatch_get_specific(databaseConnection->IsOnConnectionQueueKey))
		block();
	else
		dispatch_sync(databaseConnection->connectionQueue, block);

	return result;
}

- (void)setQueryCacheEnabled:(BOOL)queryCacheEnabled
{
	dispatch_block_t block = ^{

		if (queryCacheEnabled)
		{
			if (queryCache == nil)
				queryCache = [[YapCache alloc] initWithKeyClass:[NSString class] countLimit:queryCacheLimit];
		}
		else
		{
			queryCache = nil;
		}
	};

	if (dispatch_get_specific(databaseConnection->IsOnConnectionQueueKey))
		block();
	else
		dispatch_async(databaseConnection->connectionQueue, block);
}

- (NSUInteger)queryCacheLimit
{
	__block NSUInteger result = 0;

	dispatch_block_t block = ^{

		result = queryCacheLimit;
	};

	if (dispatch_get_specific(databaseConnection->IsOnConnectionQueueKey))
		block();
	else
		dispatch_sync(databaseConnection->connectionQueue, block);

	return result;
}

- (void)setQueryCacheLimit:(NSUInteger)newQueryCacheLimit
{
	dispatch_block_t block = ^{

		queryCacheLimit = newQueryCacheLimit;
		queryCache.countLimit = queryCacheLimit;
	};

	if (dispatch_get_specific(databaseConnection->IsOnConnectionQueueKey))
		block();
	else
		dispatch_async(databaseConnection->connectionQueue, block);
}

#pragma mark - Transactions

- (id)newReadTransaction:(YapDatabaseReadTransaction *)databaseTransaction
{
	YapDatabaseViewModelTransaction *transaction = [[YapDatabaseViewModelTransaction alloc] initWithViewModelConnection:self
                                                                                                    databaseTransaction:databaseTransaction];
	return transaction;
}

- (id)newReadWriteTransaction:(YapDatabaseReadWriteTransaction *)databaseTransaction
{
	YapDatabaseViewModelTransaction *transaction = [[YapDatabaseViewModelTransaction alloc] initWithViewModelConnection:self
                                                                                                    databaseTransaction:databaseTransaction];

	return transaction;
}

- (void)getInternalChangeset:(NSMutableDictionary **)internalChangesetPtr
           externalChangeset:(NSMutableDictionary **)externalChangesetPtr
              hasDiskChanges:(BOOL *)hasDiskChangesPtr
{}

- (void)processChangeset:(NSDictionary *)changeset
{}

#pragma mark - Statements

- (sqlite3_stmt *)getDataForRowidStatement
{
	sqlite3_stmt **statement = &getDataForRowidStatement;

        NSString *string = [NSString stringWithFormat:@"SELECT \"data\" FROM \"%@\" WHERE \"rowid\" = ?;", [viewModel tableName]];
		char *stmt = string.UTF8String;
		int stmtLen = (int)strlen(stmt);

		sqlite3 *db = databaseConnection->db;

		int status = sqlite3_prepare_v2(db, stmt, stmtLen+1, statement, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"Error creating '%@': %d %s", THIS_METHOD, status, sqlite3_errmsg(db));
		}

	return *statement;
}

- (sqlite3_stmt *)insertStatement
{
	sqlite3_stmt **statement = &insertStatement;
	if (*statement == NULL)
	{
		NSString *string = [NSString stringWithFormat:@"INSERT INTO \"%@\" (\"rowid\", \"key\", \"data\") VALUES (?,?,?);", [viewModel tableName]];

		sqlite3 *db = databaseConnection->db;

		int status = sqlite3_prepare_v2(db, [string UTF8String], -1, statement, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"%@: Error creating prepared statement: %d %s", THIS_METHOD, status, sqlite3_errmsg(db));
		}
	}

	return *statement;
}

- (sqlite3_stmt *)removeStatement
{
	sqlite3_stmt **statement = &removeStatement;
	if (*statement == NULL)
	{
		NSString *string =
        [NSString stringWithFormat:@"DELETE FROM \"%@\" WHERE \"rowid\" = ?;", [viewModel tableName]];

		sqlite3 *db = databaseConnection->db;

		int status = sqlite3_prepare_v2(db, [string UTF8String], -1, statement, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"%@: Error creating prepared statement: %d %s", THIS_METHOD, status, sqlite3_errmsg(db));
		}
	}

	return *statement;
}

- (sqlite3_stmt *)removeAllStatement
{
	sqlite3_stmt **statement = &removeAllStatement;
	if (*statement == NULL)
	{
		NSString *string = [NSString stringWithFormat:@"DELETE FROM \"%@\";", [viewModel tableName]];

		sqlite3 *db = databaseConnection->db;

		int status = sqlite3_prepare_v2(db, [string UTF8String], -1, statement, NULL);
		if (status != SQLITE_OK)
		{
			YDBLogError(@"%@: Error creating prepared statement: %d %s", THIS_METHOD, status, sqlite3_errmsg(db));
		}
	}

	return *statement;
}

@end
