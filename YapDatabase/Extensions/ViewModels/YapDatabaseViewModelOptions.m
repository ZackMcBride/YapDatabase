#import "YapDatabaseViewModelOptions.h"

@implementation YapDatabaseViewModelOptions

@synthesize allowedCollections = allowedCollections;

- (id)copyWithZone:(NSZone *)zone
{
	YapDatabaseViewModelOptions *copy = [YapDatabaseViewModelOptions new];
	copy->allowedCollections = allowedCollections;

	return copy;
}

@end
