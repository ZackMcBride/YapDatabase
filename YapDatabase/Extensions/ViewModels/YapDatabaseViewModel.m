#import "YapDatabaseViewModel.h"
#import "YapDatabaseViewModelPrivate.h"

#import "YapDatabasePrivate.h"
#import "YapDatabaseExtensionPrivate.h"

#import "YapDatabaseLogging.h"

/**
 * Define log level for this file: OFF, ERROR, WARN, INFO, VERBOSE
 * See YapDatabaseLogging.h for more information.
 **/
#if DEBUG
static const int ydbLogLevel = YDB_LOG_LEVEL_WARN;
#else
static const int ydbLogLevel = YDB_LOG_LEVEL_WARN;
#endif

@implementation YapDatabaseViewModel

+ (void)dropTablesForRegisteredName:(NSString *)registeredName
                    withTransaction:(YapDatabaseReadWriteTransaction *)transaction
                      wasPersistent:(BOOL)wasPersistent
{
	sqlite3 *db = transaction->connection->db;
	NSString *tableName = [self tableNameForRegisteredName:registeredName];

	NSString *dropTable = [NSString stringWithFormat:@"DROP TABLE IF EXISTS \"%@\";", tableName];

	int status = sqlite3_exec(db, [dropTable UTF8String], NULL, NULL, NULL);
	if (status != SQLITE_OK)
	{
		YDBLogError(@"%@ - Failed dropping table (%@): %d %s",
		            THIS_METHOD, tableName, status, sqlite3_errmsg(db));
	}
}

+ (NSString *)tableNameForRegisteredName:(NSString *)registeredName
{
	return [NSString stringWithFormat:@"viewModel_%@", registeredName];
}

- (BOOL)supportsDatabase:(YapDatabase *)database withRegisteredExtensions:(NSDictionary *)registeredExtensions
{
    return YES;
}

- (YapDatabaseExtensionConnection *)newConnection:(YapDatabaseConnection *)databaseConnection
{
	return [[YapDatabaseViewModelConnection alloc] initWithViewModel:self
                                                  databaseConnection:databaseConnection];
}

- (NSString *)tableName
{
	return [[self class] tableNameForRegisteredName:self.registeredName];
}

@synthesize versionTag = versionTag;

- (id)init
{
	NSAssert(NO, @"Must use designated initializer");
	return nil;
}

- (id)initWithSetup:(YapDatabaseViewModelSetup *)inSetup
            handler:(YapDatabaseViewModelHandler *)inHandler
{
	return [self initWithSetup:inSetup handler:inHandler versionTag:nil options:nil];
}

- (id)initWithSetup:(YapDatabaseViewModelSetup *)inSetup
            handler:(YapDatabaseViewModelHandler *)inHandler
         versionTag:(NSString *)inVersionTag
{
	return [self initWithSetup:inSetup handler:inHandler versionTag:inVersionTag options:nil];
}
- (instancetype)initWithSetup:(YapDatabaseViewModelSetup *)inSetup
                      handler:(YapDatabaseViewModelHandler *)inHandler
                   versionTag:(NSString *)inVersionTag
                      options:(YapDatabaseViewModelOptions *)inOptions
{
	// Sanity checks

	if (inSetup == nil)
	{
		NSAssert(NO, @"Invalid setup: nil");

		YDBLogError(@"%@: Invalid setup: nil", THIS_METHOD);
		return nil;
	}

	if (inHandler == NULL)
	{
		NSAssert(NO, @"Invalid handler: NULL");

		YDBLogError(@"%@: Invalid handler: NULL", THIS_METHOD);
		return nil;
	}

	// Looks sane, proceed with normal init

	if ((self = [super init]))
	{
		setup = [inSetup copy];

		block = inHandler.block;
		blockType = inHandler.blockType;
		versionTag = inVersionTag ? [inVersionTag copy] : @"";

		options = inOptions ? [inOptions copy] : [YapDatabaseViewModelOptions new];
	}
	return self;
}

@end
