#import "YapDatabaseTransaction.h"

@interface YapDatabaseReadTransactionForViewModelView : YapDatabaseReadTransaction

- (instancetype)initWithViewModelName:(NSString *)viewModelName;

@end
