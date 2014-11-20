#import <Foundation/Foundation.h>

@class YapDatabaseViewModelColumn;
@class YapDatabase;

typedef NS_ENUM(NSInteger, YapDatabaseViewModelType) {
	YapDatabaseViewModelTypeInteger,
	YapDatabaseViewModelTypeReal,
	YapDatabaseViewModelTypeText
};

@interface YapDatabaseViewModelSetup : NSObject <NSCopying>

- (instancetype)initWithRelatedCollections:(NSSet *)relatedCollections
       interpretedColumnNameForSourceCollection:(NSString *(^)(NSString *collection))interpretedColumnNameForSourceCollectionBlock
      primaryKeyForObjectInCollectionBlock:(NSString * (^)(id object, NSString *collection))primaryKeyForObjectInCollectionBlock
                 deleteViewModelForClasses:(NSSet *)classes
                           storageDatabase:(YapDatabase *)storageDatabase;

@property (nonatomic, copy, readonly) NSString *(^primaryKeyForObjectInCollection)(id object, NSString *collection);
@property (nonatomic, copy, readonly) NSString *(^interpretedColumnNameForSourceCollection)(NSString *collection);
@property (nonatomic, strong, readonly) NSSet *relatedCollections;
@property (nonatomic, strong, readonly) NSSet *deletionClasses;
@property (nonatomic, strong, readonly) YapDatabase *storageDatabase;

@end

@interface YapDatabaseViewModelColumn : NSObject

@property (nonatomic, copy, readonly) NSString *name;
@property (nonatomic, assign, readonly) YapDatabaseViewModelType type;

@end
