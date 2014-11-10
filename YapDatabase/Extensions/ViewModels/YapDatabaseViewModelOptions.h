#import <Foundation/Foundation.h>
#import "YapWhitelistBlacklist.h"

@interface YapDatabaseViewModelOptions : NSObject <NSCopying>

@property (nonatomic, strong, readwrite) YapWhitelistBlacklist *allowedCollections;

@end
