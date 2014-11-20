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

@interface YapDatabaseViewModelTransaction ()

@property (nonatomic, strong) YapDatabaseConnection *storageConnection;

@end

@implementation YapDatabaseViewModelTransaction

@synthesize databaseTransaction = databaseTransaction;

- (instancetype)initWithViewModelConnection:(YapDatabaseViewModelConnection *)inViewModelConnection
                        databaseTransaction:(YapDatabaseReadTransaction *)inDatabaseTransaction
{
    self = [super init];
    if (self) {
        viewModelConnection = inViewModelConnection;
        databaseTransaction = inDatabaseTransaction;
        _storageConnection = [viewModelConnection->viewModel->setup.storageDatabase newConnection];
    }
    return self;
}

- (BOOL)createIfNeeded
{
    return YES;
}

- (BOOL)prepareIfNeeded
{
	return YES;
}

- (BOOL)createTable
{
	return YES;
}

- (BOOL)populate
{
	[self removeAllViewModelsInCollection:viewModelConnection->viewModel.registeredName];

	// Enumerate the existing rows in the database and populate the indexes

	__unsafe_unretained YapDatabaseViewModel *viewModel = viewModelConnection->viewModel;
	__unsafe_unretained YapWhitelistBlacklist *allowedCollections = viewModel->options.allowedCollections;
    __unsafe_unretained YapDatabaseViewModelWithObjectBlock viewModelBlock = (YapDatabaseViewModelWithObjectBlock)viewModel->block;

    void (^enumBlock)(int64_t rowid, NSString *collection, NSString *key, id object, id metadata, BOOL *stop);
    enumBlock = ^(int64_t rowid, NSString *collection, NSString *key, id object, id metadata, BOOL *stop) {
        YapCollectionKey *collectionKey = [[YapCollectionKey alloc] initWithCollection:collection key:key];
        [self insertOrUpdateViewModelOnObjectChange:object forCollectionKey:collectionKey withMetadata:metadata];
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

- (void)prepareChangeset {

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

- (void)addViewModelObject:(id)object withPrimaryKey:(NSString *)primarykey collection:(NSString *)collection
{
	YDBLogAutoTrace();

    [self.storageConnection readWriteWithBlock:^(YapDatabaseReadWriteTransaction *transaction) {
        isMutated = YES;
        [transaction setObject:object forKey:primarykey inCollection:collection];
    }];
}

- (void)removeViewModelRowWithPrimaryKey:(NSString *)primaryKey inCollection:(NSString *)collection
{
    [self.storageConnection readWriteWithBlock:^(YapDatabaseReadWriteTransaction *transaction) {
        isMutated = YES;
        [transaction removeObjectForKey:primaryKey inCollection:collection];
    }];
}

- (void)removeAllViewModels
{
    [self.storageConnection readWriteWithBlock:^(YapDatabaseReadWriteTransaction *transaction) {
        [transaction removeAllObjectsInAllCollections];
    }];

	isMutated = YES;
}

- (void)removeAllViewModelsInCollection:(NSString *)viewModelCollectionName
{
    [self.storageConnection readWriteWithBlock:^(YapDatabaseReadWriteTransaction *transaction) {
        [transaction removeAllObjectsInAllCollections];
    }];

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
{
	__unsafe_unretained YapDatabaseViewModel *viewModel = viewModelConnection->viewModel;

	__unsafe_unretained NSString *collection = collectionKey.collection;
	__unsafe_unretained NSString *key = collectionKey.key;

    __unsafe_unretained YapWhitelistBlacklist *allowedCollections = viewModel->options.allowedCollections;
	if (allowedCollections && ![allowedCollections isAllowed:collection]) {
		return;
	}

    BOOL shouldProcessInsert = [viewModel->setup.relatedCollections containsObject:collection];

    if (shouldProcessInsert) {
        NSString *viewModelPrimaryKey = viewModel->setup.primaryKeyForObjectInCollection(object, collection);
        id currentViewModelObject = [self viewModelObjectForPrimaryKey:viewModelPrimaryKey];

        if (viewModel->blockType == YapDatabaseViewModelBlockTypeWithKey) {
            __unsafe_unretained YapDatabaseViewModelWithKeyBlock block =(YapDatabaseViewModelWithKeyBlock)viewModel->block;

            block(&currentViewModelObject, collection, key, self);
        } else if (viewModel->blockType == YapDatabaseViewModelBlockTypeWithObject) {
            __unsafe_unretained YapDatabaseViewModelWithObjectBlock block =
            (YapDatabaseViewModelWithObjectBlock)viewModel->block;

            block(&currentViewModelObject, collection, key, object, self);
        }
        else if (viewModel->blockType == YapDatabaseViewModelBlockTypeWithMetadata) {
            __unsafe_unretained YapDatabaseViewModelWithMetadataBlock block =
            (YapDatabaseViewModelWithMetadataBlock)viewModel->block;

            block(&currentViewModelObject, collection, key, metadata, self);
        }
        else {
            __unsafe_unretained YapDatabaseViewModelWithRowBlock block =
            (YapDatabaseViewModelWithRowBlock)viewModel->block;

            block(&currentViewModelObject, collection, key, object, metadata, self);
        }

        if (currentViewModelObject) {
            [self addViewModelObject:currentViewModelObject withPrimaryKey:viewModelPrimaryKey collection:viewModel.registeredName];
        }
    }
}

- (void)handleInsertObject:(id)object
          forCollectionKey:(YapCollectionKey *)collectionKey
              withMetadata:(id)metadata
                     rowid:(int64_t)rowid
{
    YDBLogAutoTrace();
    [self insertOrUpdateViewModelOnObjectChange:object forCollectionKey:collectionKey withMetadata:metadata];
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
    [self insertOrUpdateViewModelOnObjectChange:object forCollectionKey:collectionKey withMetadata:metadata];
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

        [self insertOrUpdateViewModelOnObjectChange:object forCollectionKey:collectionKey withMetadata:metadata];
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

        [self insertOrUpdateViewModelOnObjectChange:object forCollectionKey:collectionKey withMetadata:metadata];
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
	__unsafe_unretained YapDatabaseViewModel *viewModel = viewModelConnection->viewModel;

	__unsafe_unretained NSString *collection = collectionKey.collection;
	__unsafe_unretained NSString *key = collectionKey.key;

    __unsafe_unretained YapWhitelistBlacklist *allowedCollections = viewModel->options.allowedCollections;
	if (allowedCollections && ![allowedCollections isAllowed:collection])
    {
		return;
	}

    BOOL shouldProcessDelete = [viewModel->setup.relatedCollections containsObject:collection];
    if (shouldProcessDelete)
    {
        NSSet *deletionClasses = viewModel->setup.deletionClasses;
        if ([deletionClasses containsObject:collection])
        {
            NSString *viewModelColumnName = viewModel->setup.interpretedColumnNameForSourceCollection(collection);
            if (viewModelColumnName)
            {
                [self.storageConnection readWriteWithBlock:^(YapDatabaseReadWriteTransaction *transaction) {
                    __block NSString *viewModelKey;

                    [transaction enumerateKeysAndObjectsInCollection:viewModel.registeredName usingBlock:^(NSString *key, id object, BOOL *stop) {
                        if ([[object valueForKeyPath:viewModelColumnName] isEqualToString:key]) {
                            viewModelKey = key;
                            *stop = YES;
                        }
                    }];

                    [transaction removeObjectForKey:viewModelKey inCollection:viewModel.registeredName];
                }];

            }
        }
    }
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

    [keys enumerateObjectsUsingBlock:^(NSString *key, NSUInteger idx, BOOL *stop) {
        [self removeViewModelRowWithPrimaryKey:key inCollection:collection];
    }];
}

/**
 * YapDatabase extension hook.
 * This method is invoked by a YapDatabaseReadWriteTransaction as a post-operation-hook.
 **/
- (void)handleRemoveAllObjectsInAllCollections
{
	YDBLogAutoTrace();
    [self removeAllViewModels];
}

- (id)viewModelObjectForPrimaryKey:(NSString *)primaryKey {
    __block id object;
    [self.storageConnection readWithBlock:^(YapDatabaseReadTransaction *transaction) {
        object = [transaction objectForKey:primaryKey inCollection:viewModelConnection->viewModel.registeredName];
    }];
    return object;
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
