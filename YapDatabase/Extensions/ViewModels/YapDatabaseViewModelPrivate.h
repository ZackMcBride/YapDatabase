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

/**
 * This version number is stored in the yap2 table.
 * If there is a major re-write to this class, then the version number will be incremented,
 * and the class can automatically rebuild the table as needed.
 **/
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

    YapCache *queryCache;
    NSUInteger queryCacheLimit;
}

- (instancetype)initWithViewModel:(YapDatabaseViewModel *)viewModel
               databaseConnection:(YapDatabaseConnection *)databaseConnection;

- (sqlite3_stmt *)insertStatement;
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

/**
 * This method compares its setup to a current table structure.
 *
 * @param columns
 *
 *   Dictionary of column names and affinity.
 *
 * @see YapDatabase columnNamesAndAffinityForTable:using:
 **/
- (BOOL)matchesExistingColumnNamesAndAffinity:(NSDictionary *)columns;

@end
