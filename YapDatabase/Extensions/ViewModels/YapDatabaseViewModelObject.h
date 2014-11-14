#import <Foundation/Foundation.h>

@interface YapDatabaseViewModelObject : NSObject

@property (nonatomic, readonly) NSString *key;
@property (nonatomic, strong) id object;

+ (instancetype)withKey:(NSString *)key
                 object:(id)object;

@end
