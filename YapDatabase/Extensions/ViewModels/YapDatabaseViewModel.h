#import "YapDatabaseExtension.h"

@class YapDatabaseViewModelSetup;
@class YapDatabaseViewModelHandler;
@class YapDatabaseViewModelOptions;

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
