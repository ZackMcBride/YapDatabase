#import <Foundation/Foundation.h>

typedef NS_ENUM(NSInteger, YapDatabaseViewModelBlockType) {
	YapDatabaseViewModelBlockTypeWithKey       = 1031,
	YapDatabaseViewModelBlockTypeWithObject    = 1032,
	YapDatabaseViewModelBlockTypeWithMetadata  = 1033,
	YapDatabaseViewModelBlockTypeWithRow       = 1034
};

@interface YapDatabaseViewModelHandler : NSObject

typedef id YapDatabaseViewModelBlock; // One of the YapDatabaseSecondaryIndexWith_X_Block types below.

typedef void (^YapDatabaseViewModelWithKeyBlock)      \
(NSMutableDictionary *dict, NSString *collection, NSString *key);
typedef void (^YapDatabaseViewModelWithObjectBlock)   \
(NSMutableDictionary *dict, NSString *collection, NSString *key, id object);
typedef void (^YapDatabaseViewModelWithMetadataBlock) \
(NSMutableDictionary *dict, NSString *collection, NSString *key, id metadata);
typedef void (^YapDatabaseViewModelWithRowBlock)      \
(NSMutableDictionary *dict, NSString *collection, NSString *key, id object, id metadata);

+ (instancetype)withKeyBlock:(YapDatabaseViewModelWithKeyBlock)block;
+ (instancetype)withObjectBlock:(YapDatabaseViewModelWithObjectBlock)block;
+ (instancetype)withMetadataBlock:(YapDatabaseViewModelWithMetadataBlock)block;
+ (instancetype)withRowBlock:(YapDatabaseViewModelWithRowBlock)block;

@property (nonatomic, strong, readonly) YapDatabaseViewModelBlock block;
@property (nonatomic, assign, readonly) YapDatabaseViewModelBlockType blockType;

@end
