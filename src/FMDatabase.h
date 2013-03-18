#import <Foundation/Foundation.h>
#import "sqlite3.h"
#import "FMResultSet.h"
#import "FMDatabasePool.h"


#if ! __has_feature(objc_arc)
    #define FMDBAutorelease(__v) ([__v autorelease]);
    #define FMDBReturnAutoreleased FMDBAutorelease

    #define FMDBRetain(__v) ([__v retain]);
    #define FMDBReturnRetained FMDBRetain

    #define FMDBRelease(__v) ([__v release]);

	#define FMDBDispatchQueueRelease(__v) (dispatch_release(__v));
#else
    // -fobjc-arc
    #define FMDBAutorelease(__v)
    #define FMDBReturnAutoreleased(__v) (__v)

    #define FMDBRetain(__v)
    #define FMDBReturnRetained(__v) (__v)

    #define FMDBRelease(__v)

	#if TARGET_OS_IPHONE
		// Compiling for iOS
		#if __IPHONE_OS_VERSION_MIN_REQUIRED >= 60000
			// iOS 6.0 or later
			#define FMDBDispatchQueueRelease(__v)
		#else
			// iOS 5.X or earlier
			#define FMDBDispatchQueueRelease(__v) (dispatch_release(__v));
		#endif
	#else
		// Compiling for Mac OS X
		#if MAC_OS_X_VERSION_MIN_REQUIRED >= 1080     
			// Mac OS X 10.8 or later
			#define FMDBDispatchQueueRelease(__v)
		#else
			// Mac OS X 10.7 or earlier
			#define FMDBDispatchQueueRelease(__v) (dispatch_release(__v));
		#endif
	#endif
#endif


@interface FMDatabase : NSObject  {
    
    sqlite3*            _db;
    NSString*           _databasePath;
    BOOL                _logsErrors;
    BOOL                _crashOnErrors;
    BOOL                _traceExecution;
    BOOL                _checkedOut;
    BOOL                _shouldCacheStatements;
    BOOL                _isExecutingStatement;
    BOOL                _inTransaction;
    int                 _busyRetryTimeout;
    
    NSMutableDictionary *_cachedStatements;
    NSMutableSet        *_openResultSets;
    NSMutableSet        *_openFunctions;

    NSDateFormatter     *_dateFormat;
}


@property (atomic, assign) BOOL traceExecution;
@property (atomic, assign) BOOL checkedOut;
@property (atomic, assign) int busyRetryTimeout;
@property (atomic, assign) BOOL crashOnErrors;
@property (atomic, assign) BOOL logsErrors;
@property (atomic, retain) NSMutableDictionary *cachedStatements;

/** Set to a date formatter to use string dates with sqlite instead of the default UNIX timestamps.
 
 Set to nil to use UNIX timestamps.
 
 Defaults to nil.
 
 Should be set using [FMDatabase storeableDateFormat].
 
 */
@property (atomic, retain) NSDateFormatter *dateFormat;

/** Generate an NSDateFormat that won't be broken by timezone or locale changes.
 
 Use this method to generate values to set the dateFormat property.
 
 @param dateFormat A valid NSDateFormatter format string.
 
 Example:
 
   myDB.dateFormat = [FMDatabase storeableDateFormat:@"yyyy-MM-dd HH:mm:ss"];
 
 */
+ (NSDateFormatter *)storeableDateFormat:(NSString *)format;


+ (id)databaseWithPath:(NSString*)inPath;
- (id)initWithPath:(NSString*)inPath;

- (BOOL)open;
#if SQLITE_VERSION_NUMBER >= 3005000
- (BOOL)openWithFlags:(int)flags;
#endif
- (BOOL)close;
- (BOOL)goodConnection;
- (void)clearCachedStatements;
- (void)closeOpenResultSets;
- (BOOL)hasOpenResultSets;

// encryption methods.  You need to have purchased the sqlite encryption extensions for these to work.
- (BOOL)setKey:(NSString*)key;
- (BOOL)rekey:(NSString*)key;

- (NSString *)databasePath;

- (NSString*)lastErrorMessage;

- (int)lastErrorCode;
- (BOOL)hadError;
- (NSError*)lastError;

- (sqlite_int64)lastInsertRowId;

- (sqlite3*)sqliteHandle;

- (BOOL)update:(NSString*)sql withErrorAndBindings:(NSError**)outErr, ...;
- (BOOL)executeUpdate:(NSString*)sql, ...;
- (BOOL)executeUpdateWithFormat:(NSString *)format, ...;
- (BOOL)executeUpdate:(NSString*)sql withArgumentsInArray:(NSArray *)arguments;
- (BOOL)executeUpdate:(NSString*)sql withParameterDictionary:(NSDictionary *)arguments;

- (FMResultSet *)executeQuery:(NSString*)sql, ...;
- (FMResultSet *)executeQueryWithFormat:(NSString*)format, ...;
- (FMResultSet *)executeQuery:(NSString *)sql withArgumentsInArray:(NSArray *)arguments;
- (FMResultSet *)executeQuery:(NSString *)sql withParameterDictionary:(NSDictionary *)arguments;

- (BOOL)rollback;
- (BOOL)commit;
- (BOOL)beginTransaction;
- (BOOL)beginDeferredTransaction;
- (BOOL)inTransaction;
- (BOOL)shouldCacheStatements;
- (void)setShouldCacheStatements:(BOOL)value;

#if SQLITE_VERSION_NUMBER >= 3007000
- (BOOL)startSavePointWithName:(NSString*)name error:(NSError**)outErr;
- (BOOL)releaseSavePointWithName:(NSString*)name error:(NSError**)outErr;
- (BOOL)rollbackToSavePointWithName:(NSString*)name error:(NSError**)outErr;
- (NSError*)inSavePoint:(void (^)(BOOL *rollback))block;
#endif

+ (BOOL)isSQLiteThreadSafe;
+ (NSString*)sqliteLibVersion;

- (int)changes;

- (void)makeFunctionNamed:(NSString*)name maximumArguments:(int)count withBlock:(void (^)(sqlite3_context *context, int argc, sqlite3_value **argv))block;

@end

@interface FMStatement : NSObject {
    sqlite3_stmt *_statement;
    NSString *_query;
    long _useCount;
    BOOL _inUse;
}

@property (atomic, assign) long useCount;
@property (atomic, retain) NSString *query;
@property (atomic, assign) sqlite3_stmt *statement;
@property (atomic, assign) BOOL inUse;

- (void)close;
- (void)reset;

@end

