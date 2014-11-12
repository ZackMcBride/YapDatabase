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

@implementation YapDatabaseViewModelSetup
{
	NSMutableArray *setup;
}

- (id)init
{
	return [self initWithCapacity:0];
}

- (id)initWithCapacity:(NSUInteger)capacity
{
	self = [super init];
    if (self) {
		if (capacity > 0)
			setup = [[NSMutableArray alloc] initWithCapacity:capacity];
		else
			setup = [[NSMutableArray alloc] init];
	}
	return self;
}

- (id)initForCopy
{
	self = [super init];
	return self;
}

- (BOOL)isReservedName:(NSString *)columnName
{
	if ([columnName caseInsensitiveCompare:@"rowid"] == NSOrderedSame)
		return YES;

	if ([columnName caseInsensitiveCompare:@"oid"] == NSOrderedSame)
		return YES;

	if ([columnName caseInsensitiveCompare:@"_rowid_"] == NSOrderedSame)
		return YES;

	return NO;
}

- (BOOL)isExistingName:(NSString *)columnName
{
	// SQLite column names are not case sensitive.

	for (YapDatabaseViewModelColumn *column in setup)
	{
		if ([column.name caseInsensitiveCompare:columnName] == NSOrderedSame)
		{
			return YES;
		}
	}

	return NO;
}

- (void)addColumn:(NSString *)columnName withType:(YapDatabaseViewModelType)type
{
	if (columnName == nil)
	{
		NSAssert(NO, @"Invalid columnName: nil");

		YDBLogError(@"%@: Invalid columnName: nil", THIS_METHOD);
		return;
	}

	if ([self isReservedName:columnName])
	{
		NSAssert(NO, @"Invalid columnName: columnName is reserved");

		YDBLogError(@"Invalid columnName: columnName is reserved");
		return;
	}

	if ([self isExistingName:columnName])
	{
		NSAssert(NO, @"Invalid columnName: columnName already exists");

		YDBLogError(@"Invalid columnName: columnName already exists");
		return;
	}

	if (type != YapDatabaseViewModelTypeInteger &&
	    type != YapDatabaseViewModelTypeReal    &&
	    type != YapDatabaseViewModelTypeText)
	{
		NSAssert(NO, @"Invalid type");

		YDBLogError(@"%@: Invalid type", THIS_METHOD);
		return;
	}

	YapDatabaseViewModelColumn *column = [[YapDatabaseViewModelColumn alloc] initWithName:columnName type:type];

	[setup addObject:column];
}

- (NSUInteger)count
{
	return [setup count];
}

- (YapDatabaseViewModelColumn *)columnAtIndex:(NSUInteger)index
{
	if (index < [setup count])
		return [setup objectAtIndex:index];
	else
		return nil;
}

- (YapDatabaseViewModelColumn *)columnWithName:(NSString *)name {
    for (YapDatabaseViewModelColumn *column in setup)
	{
		if ([column.name isEqualToString:name]) {
            return column;
        }
	}
    return nil;
}

- (NSArray *)columnNames
{
	NSMutableArray *columnNames = [NSMutableArray arrayWithCapacity:[setup count]];

	for (YapDatabaseViewModelColumn *column in setup)
	{
		[columnNames addObject:column.name];
	}

	return [columnNames copy];
}

- (id)copyWithZone:(NSZone *)zone
{
	YapDatabaseViewModelSetup *copy = [[YapDatabaseViewModelSetup alloc] initForCopy];
	copy->setup = [setup mutableCopy];

	return copy;
}

- (NSUInteger)countByEnumeratingWithState:(NSFastEnumerationState *)state
                                  objects:(__unsafe_unretained id [])buffer
                                    count:(NSUInteger)length
{
	return [setup countByEnumeratingWithState:state objects:buffer count:length];
}

- (BOOL)matchesExistingColumnNamesAndAffinity:(NSDictionary *)columns
{
	// The columns parameter will include the 'rowid' column, which we need to ignore.

	if (([setup count] + 1) != [columns count])
	{
		return NO;
	}

	for (YapDatabaseViewModelColumn *setupColumn in setup)
	{
		NSString *existingAffinity = [columns objectForKey:setupColumn.name];
		if (existingAffinity == nil)
		{
			return NO;
		}
		else
		{
			if (setupColumn.type == YapDatabaseViewModelTypeInteger)
			{
				if ([existingAffinity caseInsensitiveCompare:@"INTEGER"] != NSOrderedSame)
					return NO;
			}
			else if (setupColumn.type == YapDatabaseViewModelTypeReal)
			{
				if ([existingAffinity caseInsensitiveCompare:@"REAL"] != NSOrderedSame)
					return NO;
			}
			else if (setupColumn.type == YapDatabaseViewModelTypeText)
			{
				if ([existingAffinity caseInsensitiveCompare:@"TEXT"] != NSOrderedSame)
					return NO;
			}
		}
	}

	return YES;
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

	return [NSString stringWithFormat:@"<YapDatabaseSecondaryIndexColumn: name(%@), type(%@)>", name, typeStr];
}

@end
