#import "YapDatabaseViewModelObject.h"

@implementation YapDatabaseViewModelObject

+ (instancetype)withKey:(NSString *)key object:(id)object
{
    return [[YapDatabaseViewModelObject alloc] initWithKey:key object:object];
}

- (instancetype)initWithKey:(NSString *)key
                     object:(id)object
{
    self = [super init];
    if (self) {
        _key = key;
        _object = object;
    }
    return self;
}

@end
