#import "YapDatabaseViewTransaction.h"

@class YapDatabaseViewConnection;

@interface YapDatabaseViewModelViewTransaction : YapDatabaseViewTransaction

- (instancetype)initWithViewConnection:(YapDatabaseViewConnection *)viewConnection
                   databaseTransaction:(YapDatabaseReadTransaction *)databaseTransaction
                         viewModelName:(NSString *)viewModelName;

@end
