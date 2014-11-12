#import "YapDatabaseExtensionConnection.h"

@class YapDatabaseViewModel;

@interface YapDatabaseViewModelConnection : YapDatabaseExtensionConnection

@property (nonatomic, strong, readonly) YapDatabaseViewModel *viewModel;

@end
