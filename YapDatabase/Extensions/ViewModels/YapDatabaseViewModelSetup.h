#import <Foundation/Foundation.h>

@class YapDatabaseViewModelColumn;

typedef NS_ENUM(NSInteger, YapDatabaseViewModelType) {
	YapDatabaseViewModelTypeInteger,
	YapDatabaseViewModelTypeReal,
	YapDatabaseViewModelTypeText
};

@interface YapDatabaseViewModelSetup : NSObject <NSCopying, NSFastEnumeration>

- (id)init;
- (id)initWithCapacity:(NSUInteger)capacity;

- (void)addColumn:(NSString *)name withType:(YapDatabaseViewModelType)type;

- (NSUInteger)count;
- (YapDatabaseViewModelColumn *)columnAtIndex:(NSUInteger)index;

- (NSArray *)columnNames;

@end

@interface YapDatabaseViewModelColumn : NSObject

@property (nonatomic, copy, readonly) NSString *name;
@property (nonatomic, assign, readonly) YapDatabaseViewModelType type;

@end