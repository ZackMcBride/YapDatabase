#import "YapDatabaseExtension.h"

#import "YapDatabaseExtensionTransaction.h"
#import "YapDatabaseQuery.h"

@class YapDatabaseReadTransaction;

@interface YapDatabaseViewModelTransaction : YapDatabaseExtensionTransaction

@property (nonatomic, weak, readonly) YapDatabaseReadTransaction *databaseTransaction;

- (NSDictionary *)rowDictionaryForColumn:(NSString *)column
                               withValue:(id)value;

- (id)viewModelObjectForPrimaryKey:(NSString *)primaryKey;
- (void)writeViewModelObject:(id)object forPrimaryKey:(NSString *)primaryKey;

@end
