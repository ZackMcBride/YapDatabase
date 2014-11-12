#import "YapDatabaseExtension.h"

#import "YapDatabaseExtensionTransaction.h"
#import "YapDatabaseQuery.h"

@interface YapDatabaseViewModelTransaction : YapDatabaseExtensionTransaction

- (int64_t)rowIdForRowWithValue:(id)value inColumn:(NSString *)column;

@end
