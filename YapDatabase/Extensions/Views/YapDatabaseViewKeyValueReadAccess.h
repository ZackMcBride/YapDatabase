#import <Foundation/Foundation.h>

@class YapCollectionKey;
@class YapDatabaseReadTransaction;

@interface YapDatabaseViewKeyValueReadAccess : NSObject
{
    @protected
    YapDatabaseReadTransaction *databaseTransaction;
}

- (id)initWithDatabaseTransaction:(YapDatabaseReadTransaction *)transaction;

- (id)our_metadataForCollectionKey:(YapCollectionKey *)cacheKey withRowid:(int64_t)rowid;
- (YapCollectionKey *)our_collectionKeyForRowid:(int64_t)rowid;
- (id)our_objectForCollectionKey:(YapCollectionKey *)cacheKey withRowid:(int64_t)rowid;
- (BOOL)our_getCollectionKey:(YapCollectionKey **)collectionKeyPtr object:(id *)objectPtr forRowid:(int64_t)rowid;
- (BOOL)our_getCollectionKey:(YapCollectionKey **)collectionKeyPtr metadata:(id *)metadataPtr forRowid:(int64_t)rowid;
- (BOOL)our_getCollectionKey:(YapCollectionKey **)collectionKeyPtr
                      object:(id *)objectPtr
                    metadata:(id *)metadataPtr
                    forRowid:(int64_t)rowid;
- (BOOL)our_getRowid:(int64_t *)rowidPtr forKey:(NSString *)key inCollection:(NSString *)collection;
- (void)our_enumerateCollectionsUsingBlock:(void (^)(NSString *collection, BOOL *stop))block;
- (void)our__enumerateRowsInCollections:(NSArray *)collections usingBlock:
(void (^)(int64_t rowid, NSString *collection, NSString *key, id object, id metadata, BOOL *stop))block;
- (void)our__enumerateRowsInAllCollectionsUsingBlock:
(void (^)(int64_t rowid, NSString *collection, NSString *key, id object, id metadata, BOOL *stop))block;
- (void)our__enumerateRowsInCollections:(NSArray *)collections
                             usingBlock:(void (^)(int64_t rowid, NSString *collection, NSString *key, id object, id metadata, BOOL *stop))block
                             withFilter:(BOOL (^)(int64_t rowid, NSString *collection, NSString *key))filter;
- (void)our__enumerateRowsInAllCollectionsUsingBlock:
(void (^)(int64_t rowid, NSString *collection, NSString *key, id object, id metadata, BOOL *stop))block
                                          withFilter:(BOOL (^)(int64_t rowid, NSString *collection, NSString *key))filter;
- (void)our__enumerateKeysAndObjectsInCollections:(NSArray *)collections usingBlock:
(void (^)(int64_t rowid, NSString *collection, NSString *key, id object, BOOL *stop))block;
- (void)our__enumerateKeysAndObjectsInAllCollectionsUsingBlock:
(void (^)(int64_t rowid, NSString *collection, NSString *key, id object, BOOL *stop))block;
- (void)our__enumerateKeysAndObjectsInCollections:(NSArray *)collections
                                       usingBlock:(void (^)(int64_t rowid, NSString *collection, NSString *key, id object, BOOL *stop))block
                                       withFilter:(BOOL (^)(int64_t rowid, NSString *collection, NSString *key))filter;
- (void)our__enumerateKeysAndObjectsInAllCollectionsUsingBlock:
(void (^)(int64_t rowid, NSString *collection, NSString *key, id object, BOOL *stop))block
                                                    withFilter:(BOOL (^)(int64_t rowid, NSString *collection, NSString *key))filter;
- (void)our__enumerateKeysAndMetadataInCollections:(NSArray *)collections
                                        usingBlock:(void (^)(int64_t rowid, NSString *collection, NSString *key, id metadata, BOOL *stop))block;
- (void)our__enumerateKeysAndMetadataInCollections:(NSArray *)collections
                                        usingBlock:(void (^)(int64_t rowid, NSString *collection, NSString *key, id metadata, BOOL *stop))block
                                        withFilter:(BOOL (^)(int64_t rowid, NSString *collection, NSString *key))filter;
- (void)our__enumerateKeysAndMetadataInAllCollectionsUsingBlock:
(void (^)(int64_t rowid, NSString *collection, NSString *key, id metadata, BOOL *stop))block;
- (void)our__enumerateKeysAndMetadataInAllCollectionsUsingBlock:
(void (^)(int64_t rowid, NSString *collection, NSString *key, id metadata, BOOL *stop))block
                                                     withFilter:(BOOL (^)(int64_t rowid, NSString *collection, NSString *key))filter;
- (void)our__enumerateKeysInCollections:(NSArray *)collections
                             usingBlock:(void (^)(int64_t rowid, NSString *collection, NSString *key, BOOL *stop))block;
- (void)our__enumerateKeysInAllCollectionsUsingBlock:
(void (^)(int64_t rowid, NSString *collection, NSString *key, BOOL *stop))block;
- (BOOL)our_getObject:(id *)objectPtr
             metadata:(id *)metadataPtr
     forCollectionKey:(YapCollectionKey *)collectionKey
            withRowid:(int64_t)rowid;

@end
