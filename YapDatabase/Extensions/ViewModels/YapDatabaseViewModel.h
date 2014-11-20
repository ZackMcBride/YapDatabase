#import "YapDatabaseExtension.h"

#import "YapDatabaseViewModelSetup.h"
#import "YapDatabaseViewModelHandler.h"
#import "YapDatabaseViewModelOptions.h"

@interface YapDatabaseViewModel : YapDatabaseExtension

/**
 * Creates a new view model extension.
 * After creation, you'll need to register the extension with the database system.
 *
 * @param setup
 *
 *   A YapDatabaseViewModelSetup instance allows you to specify the column names and type.
 *   The column names can be whatever you want, with a few exceptions for reserved names such as "rowid".
 *   The types can reflect numbers or text.
 *
 * @param handler
 *
 *   The block (and blockType) that handles extracting view model information from a row in the database.
 *
 *
 * @see YapDatabaseViewModelSetup
 * @see YapDatabaseViewModelHandler
 *
 * @see YapDatabase registerExtension:withName:
 **/
- (instancetype)initWithSetup:(YapDatabaseViewModelSetup *)setup
                      handler:(YapDatabaseViewModelHandler *)handler;

/**
 * Creates a new view model extension.
 * After creation, you'll need to register the extension with the database system.
 *
 * @param setup
 *
 *   A YapDatabaseViewModelSetup instance allows you to specify the column names and type.
 *   The column names can be whatever you want, with a few exceptions for reserved names such as "rowid".
 *   The types can reflect numbers or text.
 *
 * @param handler
 *
 *   The block (and blockType) that handles extracting view model information from a row in the database.
 *
 * @param version
 *
 *   If, after creating the view model(s), you need to change the setup or block,
 *   then simply increment the version parameter. If you pass a version that is different from the last
 *   initialization of the extension, then it will automatically re-create itself.
 *
 * @see YapDatabaseViewModelSetup
 * @see YapDatabaseViewModelHandler
 *
 * @see YapDatabase registerExtension:withName:
 **/
- (instancetype)initWithSetup:(YapDatabaseViewModelSetup *)setup
                      handler:(YapDatabaseViewModelHandler *)handler
                   versionTag:(NSString *)versionTag;

/**
 * Creates a new view model extension.
 * After creation, you'll need to register the extension with the database system.
 *
 * @param setup
 *
 *   A YapDatabaseViewModelSetup instance allows you to specify the column names and type.
 *   The column names can be whatever you want, with a few exceptions for reserved names such as "rowid".
 *   The types can reflect numbers or text.
 *
 * @param handler
 *
 *   The block (and blockType) that handles extracting view model information from a row in the database.
 *
 * @param version
 *
 *   If, after creating the view model(s), you need to change the setup or block,
 *   then simply increment the version parameter. If you pass a version that is different from the last
 *   initialization of the extension, then it will automatically re-create itself.
 *
 * @param options
 *
 *   Allows you to specify extra options to configure the extension.
 *   See the YapDatabaseViewModelOptions class for more information.
 *
 * @see YapDatabaseViewModelSetup
 * @see YapDatabaseViewModelHandler
 *
 * @see YapDatabase registerExtension:withName:
 **/
- (instancetype)initWithSetup:(YapDatabaseViewModelSetup *)setup
                      handler:(YapDatabaseViewModelHandler *)handler
                   versionTag:(NSString *)versionTag
                      options:(YapDatabaseViewModelOptions *)options;

/**
 * The versionTag assists in making changes to the extension.
 *
 * If you need to change the columnNames and/or block,
 * then simply pass a different versionTag during the init method,
 * and the extension will automatically update itself.
 **/
@property (nonatomic, copy, readonly) NSString *versionTag;

+ (NSString *)tableNameForRegisteredName:(NSString *)registeredName;

@end
