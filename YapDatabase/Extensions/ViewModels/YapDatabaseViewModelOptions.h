#import <Foundation/Foundation.h>
#import "YapWhitelistBlacklist.h"

@interface YapDatabaseViewModelOptions : NSObject <NSCopying>

/**
 * For all rows whose collection is in the allowedCollections, the extension acts normally.
 * So the viewModelBlock would still be invoked as normal.
 *
 * The default value is nil.
 **/
@property (nonatomic, strong, readwrite) YapWhitelistBlacklist *allowedCollections;

@end
