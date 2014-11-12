#import "YapDatabase.h"
#import "YapDatabaseConnection.h"
#import "YapDatabaseTransaction.h"

#import "YapDatabaseViewModel.h"
#import "YapDatabaseViewModelConnection.h"
#import "YapDatabaseViewModelTransaction.h"
#import "YapDatabaseViewModelSetup.h"
#import "YapDatabaseViewModelOptions.h"
#import "YapDatabaseViewModelHandler.h"

#import "YapCache.h"
#import "sqlite3.h"

#define YAP_DATABASE_VIEW_MODEL_CLASS_VERSION 1

@interface YapDatabaseViewModel () {
@public
    id columnNamesSharedKeySet;

    YapDatabaseViewModelSetup *setup;
    YapDatabaseViewModelOptions *options;

    YapDatabaseViewModelBlock block;
	YapDatabaseViewModelBlockType blockType;

    NSString *versionTag;
}

- (NSString *)tableName;

@end

@interface YapDatabaseViewModelConnection () {
@public
    __strong YapDatabaseViewModel *viewModel;
    __unsafe_unretained YapDatabaseConnection *databaseConnection;

    NSMutableDictionary *blockDict;

    YapCache *queryCache;
    NSUInteger queryCacheLimit;
}

- (instancetype)initWithViewModel:(YapDatabaseViewModel *)viewModel
               databaseConnection:(YapDatabaseConnection *)databaseConnection;

- (sqlite3_stmt *)insertStatement;
- (sqlite3_stmt *)updateStatementWithColumns:(NSArray *)columns;
- (sqlite3_stmt *)removeStatement;
- (sqlite3_stmt *)removeAllStatement;

@end

@interface YapDatabaseViewModelTransaction () {
@private
    __unsafe_unretained YapDatabaseViewModelConnection *viewModelConnection;
    __unsafe_unretained YapDatabaseReadTransaction *databaseTransaction;

    BOOL isMutated;
}

- (id)initWithViewModelConnection:(YapDatabaseViewModelConnection *)viewModelConnection
                   databaseTransaction:(YapDatabaseReadTransaction *)databaseTransaction;

@end

@interface YapDatabaseViewModelSetup ()

- (BOOL)matchesExistingColumnNamesAndAffinity:(NSDictionary *)columns;

@end
