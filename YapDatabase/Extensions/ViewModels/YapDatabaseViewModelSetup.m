#import "YapDatabaseViewModelSetup.h"
#import "YapDatabaseLogging.h"

#if ! __has_feature(objc_arc)
#warning This file must be compiled with ARC. Use -fobjc-arc flag (or convert project to ARC).
#endif

#if DEBUG
static const int ydbLogLevel = YDB_LOG_LEVEL_WARN;
#else
static const int ydbLogLevel = YDB_LOG_LEVEL_WARN;
#endif

@interface YapDatabaseViewModelColumn ()

- (id)initWithName:(NSString *)name type:(YapDatabaseViewModelType)type;

@end

@interface YapDatabaseViewModelSetup ()

@property (nonatomic, copy) NSString *(^primaryKeyForObjectInCollection)(id object, NSString *collection);
@property (nonatomic, strong) NSSet *relatedCollections;
@property (nonatomic, strong) NSSet *deletionClasses;

@end

@implementation YapDatabaseViewModelSetup

- (instancetype)initWithRelatedCollections:(NSSet *)relatedCollections
      primaryKeyForObjectInCollectionBlock:(NSString *(^)(id, NSString *))primaryKeyForObjectInCollectionBlock
                 deleteViewModelForClasses:(NSSet *)classes
{
    self = [super init];
    if (self) {
        _relatedCollections = relatedCollections;
        _primaryKeyForObjectInCollection = primaryKeyForObjectInCollectionBlock;
        _deletionClasses = classes;
    }
    return self;
}

- (id)initForCopy
{
	self = [super init];
	return self;
}

- (id)copyWithZone:(NSZone *)zone
{
	YapDatabaseViewModelSetup *copy = [[YapDatabaseViewModelSetup alloc] initForCopy];
	copy.relatedCollections = [self.relatedCollections copy];
    copy.primaryKeyForObjectInCollection = self.primaryKeyForObjectInCollection;
    copy.deletionClasses = self.deletionClasses;

	return copy;
}

@end

@implementation YapDatabaseViewModelColumn

@synthesize name = name;
@synthesize type = type;

- (id)initWithName:(NSString *)inName type:(YapDatabaseViewModelType)inType
{
	if ((self = [super init]))
	{
		name = [inName copy];
		type = inType;
	}
	return self;
}

- (NSString *)description
{
	NSString *typeStr;
	if (type == YapDatabaseViewModelTypeInteger) {
		typeStr = @"Integer";
    } else if (type == YapDatabaseViewModelTypeReal) {
		typeStr = @"Real";
    } else if (type == YapDatabaseViewModelTypeText) {
		typeStr = @"Text";
    } else {
		typeStr = @"Unknown";
    }

	return [NSString stringWithFormat:@"<YapDatabaseViewModelColumn: name(%@), type(%@)>", name, typeStr];
}

@end
