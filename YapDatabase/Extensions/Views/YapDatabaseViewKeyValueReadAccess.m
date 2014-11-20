#import "YapDatabaseViewKeyValueReadAccess.h"

#import "YapCollectionKey.h"
#import "YapDatabasePrivate.h"
#import "YapDatabaseTransaction.h"

@implementation YapDatabaseViewKeyValueReadAccess

- (id)initWithDatabaseTransaction:(YapDatabaseReadTransaction *)transaction {
    self = [super init];
    if (self) {
        databaseTransaction = transaction;
    }
    return self;
}

- (id)our_metadataForCollectionKey:(YapCollectionKey *)cacheKey withRowid:(int64_t)rowid
{
    return [databaseTransaction metadataForCollectionKey:cacheKey withRowid:rowid];
}

- (YapCollectionKey *)our_collectionKeyForRowid:(int64_t)rowid
{
    return [databaseTransaction collectionKeyForRowid:rowid];
}

- (id)our_objectForCollectionKey:(YapCollectionKey *)cacheKey withRowid:(int64_t)rowid
{
    return [databaseTransaction objectForCollectionKey:cacheKey withRowid:rowid];
}

- (BOOL)our_getCollectionKey:(YapCollectionKey **)collectionKeyPtr object:(id *)objectPtr forRowid:(int64_t)rowid
{
    return [databaseTransaction getCollectionKey:collectionKeyPtr object:objectPtr forRowid:rowid];
}

- (BOOL)our_getCollectionKey:(YapCollectionKey **)collectionKeyPtr metadata:(id *)metadataPtr forRowid:(int64_t)rowid
{
    return [databaseTransaction getCollectionKey:collectionKeyPtr metadata:metadataPtr forRowid:rowid];
}

- (BOOL)our_getCollectionKey:(YapCollectionKey **)collectionKeyPtr
                      object:(id *)objectPtr
                    metadata:(id *)metadataPtr
                    forRowid:(int64_t)rowid
{
    return [databaseTransaction getCollectionKey:collectionKeyPtr object:objectPtr metadata:metadataPtr forRowid:rowid];
}

- (BOOL)our_getRowid:(int64_t *)rowidPtr forKey:(NSString *)key inCollection:(NSString *)collection
{
    return [databaseTransaction getRowid:rowidPtr forKey:key inCollection:collection];
}

- (void)our_enumerateCollectionsUsingBlock:(void (^)(NSString *collection, BOOL *stop))block
{
    [databaseTransaction enumerateCollectionsUsingBlock:block];
}

- (void)our__enumerateRowsInCollections:(NSArray *)collections usingBlock:
(void (^)(int64_t rowid, NSString *collection, NSString *key, id object, id metadata, BOOL *stop))block
{
    [databaseTransaction _enumerateRowsInCollections:collections usingBlock:block];
}

- (void)our__enumerateRowsInAllCollectionsUsingBlock:
(void (^)(int64_t rowid, NSString *collection, NSString *key, id object, id metadata, BOOL *stop))block
{
    [databaseTransaction _enumerateRowsInAllCollectionsUsingBlock:block];
}

- (void)our__enumerateRowsInCollections:(NSArray *)collections
                             usingBlock:(void (^)(int64_t rowid, NSString *collection, NSString *key, id object, id metadata, BOOL *stop))block
                             withFilter:(BOOL (^)(int64_t rowid, NSString *collection, NSString *key))filter
{
    [databaseTransaction _enumerateRowsInCollections:collections usingBlock:block withFilter:filter];
}

- (void)our__enumerateRowsInAllCollectionsUsingBlock:
(void (^)(int64_t rowid, NSString *collection, NSString *key, id object, id metadata, BOOL *stop))block
                                          withFilter:(BOOL (^)(int64_t rowid, NSString *collection, NSString *key))filter
{
    [databaseTransaction _enumerateRowsInAllCollectionsUsingBlock:block withFilter:filter];
}

- (void)our__enumerateKeysAndObjectsInCollections:(NSArray *)collections usingBlock:
(void (^)(int64_t rowid, NSString *collection, NSString *key, id object, BOOL *stop))block
{
    [databaseTransaction _enumerateKeysAndObjectsInCollections:collections usingBlock:block];
}

- (void)our__enumerateKeysAndObjectsInAllCollectionsUsingBlock:
(void (^)(int64_t rowid, NSString *collection, NSString *key, id object, BOOL *stop))block
{
    [databaseTransaction _enumerateKeysAndObjectsInAllCollectionsUsingBlock:block];
}

- (void)our__enumerateKeysAndObjectsInCollections:(NSArray *)collections
                                       usingBlock:(void (^)(int64_t rowid, NSString *collection, NSString *key, id object, BOOL *stop))block
                                       withFilter:(BOOL (^)(int64_t rowid, NSString *collection, NSString *key))filter
{
    [databaseTransaction _enumerateKeysAndObjectsInCollections:collections usingBlock:block withFilter:filter];
}

- (void)our__enumerateKeysAndObjectsInAllCollectionsUsingBlock:
(void (^)(int64_t rowid, NSString *collection, NSString *key, id object, BOOL *stop))block
                                                    withFilter:(BOOL (^)(int64_t rowid, NSString *collection, NSString *key))filter
{
    [databaseTransaction _enumerateKeysAndObjectsInAllCollectionsUsingBlock:block withFilter:filter];
}

- (void)our__enumerateKeysAndMetadataInCollections:(NSArray *)collections
                                        usingBlock:(void (^)(int64_t rowid, NSString *collection, NSString *key, id metadata, BOOL *stop))block
{
    [databaseTransaction _enumerateKeysAndMetadataInCollections:collections usingBlock:block];
}


- (void)our__enumerateKeysAndMetadataInCollections:(NSArray *)collections
                                        usingBlock:(void (^)(int64_t rowid, NSString *collection, NSString *key, id metadata, BOOL *stop))block
                                        withFilter:(BOOL (^)(int64_t rowid, NSString *collection, NSString *key))filter
{
    [databaseTransaction _enumerateKeysAndMetadataInCollections:collections usingBlock:block withFilter:filter];
}

- (void)our__enumerateKeysAndMetadataInAllCollectionsUsingBlock:
(void (^)(int64_t rowid, NSString *collection, NSString *key, id metadata, BOOL *stop))block
{
    [databaseTransaction _enumerateKeysAndMetadataInAllCollectionsUsingBlock:block];
}

- (void)our__enumerateKeysAndMetadataInAllCollectionsUsingBlock:
(void (^)(int64_t rowid, NSString *collection, NSString *key, id metadata, BOOL *stop))block
                                                     withFilter:(BOOL (^)(int64_t rowid, NSString *collection, NSString *key))filter
{
    [databaseTransaction _enumerateKeysAndMetadataInAllCollectionsUsingBlock:block withFilter:filter];
}

- (void)our__enumerateKeysInCollections:(NSArray *)collections
                             usingBlock:(void (^)(int64_t rowid, NSString *collection, NSString *key, BOOL *stop))block
{
    [databaseTransaction _enumerateKeysInCollections:collections usingBlock:block];
}

- (void)our__enumerateKeysInAllCollectionsUsingBlock:
(void (^)(int64_t rowid, NSString *collection, NSString *key, BOOL *stop))block
{
    [databaseTransaction _enumerateKeysInAllCollectionsUsingBlock:block];
}

- (BOOL)our_getObject:(id *)objectPtr
             metadata:(id *)metadataPtr
     forCollectionKey:(YapCollectionKey *)collectionKey
            withRowid:(int64_t)rowid
{
    return [databaseTransaction getObject:objectPtr metadata:metadataPtr forCollectionKey:collectionKey withRowid:rowid];
}


@end
