#import "YapDatabaseTransaction.h"

@interface YapDatabaseReadTransactionForViewModelView : YapDatabaseReadTransaction

- (instancetype)initWithViewModelName:(NSString *)viewModelName connection:(YapDatabaseConnection *)aConnection isReadWriteTransaction:(BOOL)flag;

@end
