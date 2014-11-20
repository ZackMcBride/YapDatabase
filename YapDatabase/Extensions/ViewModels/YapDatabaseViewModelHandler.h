#import <Foundation/Foundation.h>

@class YapDatabaseViewModelTransaction;

/**
 * Specifies the kind of block being used.
 **/
typedef NS_ENUM(NSInteger, YapDatabaseViewModelBlockType) {
	YapDatabaseViewModelBlockTypeWithKey       = 1031,
	YapDatabaseViewModelBlockTypeWithObject    = 1032,
	YapDatabaseViewModelBlockTypeWithMetadata  = 1033,
	YapDatabaseViewModelBlockTypeWithRow       = 1034
};

/**
 * The handler block handles extracting the column values for the view models.
 *
 * When you add or update rows in the database the block is invoked.
 * Your block can inspect the row and determine if it contains any values that should be added to the view models.
 * If not, the  block can simply return.
 * Otherwise the block should extract any values and add them to the given dictionary.
 *
 * After the block returns, the dictionary parameter will be inspected,
 * and any set values will be automatically inserted/updated within the sqlite indexes.
 *
 * You should choose a block type that takes the minimum number of required parameters.
 * The extension can make various optimizations based on required parameters of the block.
 * For example, if metadata isn't required, then the extension can ignore metadata-only updates.
 **/
@interface YapDatabaseViewModelHandler : NSObject

typedef id YapDatabaseViewModelBlock; // One of the YapDatabaseViewModelWith_X_Block types below.

typedef void (^YapDatabaseViewModelWithKeyBlock)      \
(id *viewModelObject, NSString *collection, NSString *key, YapDatabaseViewModelTransaction *transaction);
typedef void (^YapDatabaseViewModelWithObjectBlock)   \
(id *viewModelObject, NSString *collection, NSString *key, id object, YapDatabaseViewModelTransaction *transaction);
typedef void (^YapDatabaseViewModelWithMetadataBlock) \
(id *viewModelObject, NSString *collection, NSString *key, id metadata, YapDatabaseViewModelTransaction *transaction);
typedef void (^YapDatabaseViewModelWithRowBlock)      \
(id *viewModelObject, NSString *collection, NSString *key, id object, id metadata, YapDatabaseViewModelTransaction *transaction);

+ (instancetype)withKeyBlock:(YapDatabaseViewModelWithKeyBlock)block;
+ (instancetype)withObjectBlock:(YapDatabaseViewModelWithObjectBlock)block;
+ (instancetype)withMetadataBlock:(YapDatabaseViewModelWithMetadataBlock)block;
+ (instancetype)withRowBlock:(YapDatabaseViewModelWithRowBlock)block;

@property (nonatomic, strong, readonly) YapDatabaseViewModelBlock block;
@property (nonatomic, assign, readonly) YapDatabaseViewModelBlockType blockType;

@end
