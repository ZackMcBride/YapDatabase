#import "YapDatabaseExtension.h"

#import "YapDatabaseViewModelSetup.h"
#import "YapDatabaseViewModelHandler.h"
#import "YapDatabaseViewModelOptions.h"

@interface YapDatabaseViewModel : YapDatabaseExtension

- (instancetype)initWithSetup:(YapDatabaseViewModelSetup *)setup
                      handler:(YapDatabaseViewModelHandler *)handler;

- (instancetype)initWithSetup:(YapDatabaseViewModelSetup *)setup
                      handler:(YapDatabaseViewModelHandler *)handler
                   versionTag:(NSString *)versionTag;

- (instancetype)initWithSetup:(YapDatabaseViewModelSetup *)setup
                      handler:(YapDatabaseViewModelHandler *)handler
                   versionTag:(NSString *)versionTag
                      options:(YapDatabaseViewModelOptions *)options;

@property (nonatomic, copy, readonly) NSString *versionTag;

@end
