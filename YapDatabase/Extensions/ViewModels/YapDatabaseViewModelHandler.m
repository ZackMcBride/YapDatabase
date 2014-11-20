#import "YapDatabaseViewModelHandler.h"

@implementation YapDatabaseViewModelHandler

@synthesize block = block;
@synthesize blockType = blockType;

+ (instancetype)withKeyBlock:(YapDatabaseViewModelWithKeyBlock)block
{
	if (block == NULL) return nil;

	YapDatabaseViewModelHandler *handler = [YapDatabaseViewModelHandler new];
	handler->block = block;
	handler->blockType = YapDatabaseViewModelBlockTypeWithKey;

	return handler;
}

+ (instancetype)withObjectBlock:(YapDatabaseViewModelWithObjectBlock)block
{
	if (block == NULL) return nil;

	YapDatabaseViewModelHandler *handler = [YapDatabaseViewModelHandler new];
	handler->block = block;
	handler->blockType = YapDatabaseViewModelBlockTypeWithObject;

	return handler;
}

+ (instancetype)withMetadataBlock:(YapDatabaseViewModelWithMetadataBlock)block
{
	if (block == NULL) return nil;

	YapDatabaseViewModelHandler *handler = [YapDatabaseViewModelHandler new];
	handler->block = block;
	handler->blockType = YapDatabaseViewModelBlockTypeWithMetadata;

	return handler;
}

+ (instancetype)withRowBlock:(YapDatabaseViewModelWithRowBlock)block
{
	if (block == NULL) return nil;

	YapDatabaseViewModelHandler *handler = [YapDatabaseViewModelHandler new];
	handler->block = block;
	handler->blockType = YapDatabaseViewModelBlockTypeWithRow;

	return handler;
}

@end
