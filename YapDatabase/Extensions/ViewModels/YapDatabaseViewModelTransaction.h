#import "YapDatabaseExtension.h"

#import "YapDatabaseExtensionTransaction.h"
#import "YapDatabaseQuery.h"

@interface YapDatabaseViewModelTransaction : YapDatabaseExtensionTransaction

- (NSDictionary *)rowDictionaryForColumn:(NSString *)column
                               withValue:(id)value;

- (id)viewModelObjectForPrimaryKey:(NSString *)primaryKey;
- (void)writeViewModelObject:(id)object forPrimaryKey:(NSString *)primaryKey;

@end
