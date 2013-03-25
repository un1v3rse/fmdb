#import "FMDatabase.h"
#import "unistd.h"
#import <objc/runtime.h>
#import "FMDBLog.h"


static const NSTimeInterval FMDB_BUSY_TIMEOUT = 0.5;
static const NSTimeInterval FMDB_CLOSE_TIMEOUT = 5.0;
static const NSTimeInterval FMDB_STALE_INTERVAL = 0.5;

#ifndef DEBUG
static FMDB_LOG_T_UNITS QUERY_TIMING_CUTOFF = 0;
#endif // ! DEBUG

@interface FMDatabase ()

- (FMResultSet *)executeQuery:(NSString *)sql withArgumentsInArray:(NSArray*)arrayArgs orDictionary:(NSDictionary *)dictionaryArgs orVAList:(va_list)args;
- (BOOL)executeUpdate:(NSString*)sql error:(NSError**)outErr withArgumentsInArray:(NSArray*)arrayArgs orDictionary:(NSDictionary *)dictionaryArgs orVAList:(va_list)args;
@end

@implementation FMDatabase
@synthesize cachedStatements=_cachedStatements;
@synthesize logsErrors=_logsErrors;
@synthesize crashOnErrors=_crashOnErrors;
@synthesize busyRetryTimeout=_busyRetryTimeout;
@synthesize checkedOut=_checkedOut;
@synthesize traceExecution=_traceExecution;

+ (void)initialize {
    // set sqlite to be thread safe on the connection
    
    //QUERY_TIMING_CUTOFF = [Log t_units_from_interval:0.1];
    
    sqlite3_shutdown();
    if (sqlite3_threadsafe() > 0) {
        int retCode = sqlite3_config(SQLITE_CONFIG_SERIALIZED);
        if (retCode == SQLITE_OK) {
            FMDB_LOG_I(@"Can now use sqlite on multiple threads, using the same connection");
        } else {
            FMDB_LOG_EF(@"setting sqlite thread safe mode to serialized failed!!! return code: %d", retCode);
        }
    } else {
        FMDB_LOG_E(@"Your SQLite database is not compiled to be threadsafe.");
    }
    sqlite3_initialize();
}

+ (id)databaseWithPath:(NSString*)aPath {
    return FMDBReturnAutoreleased([[self alloc] initWithPath:aPath]);
}

+ (NSString*)sqliteLibVersion {
    return [NSString stringWithFormat:@"%s", sqlite3_libversion()];
}

+ (BOOL)isSQLiteThreadSafe {
    // make sure to read the sqlite headers on this guy!
    return sqlite3_threadsafe() != 0;
}

- (id)initWithPath:(NSString*)aPath {
    
    assert(sqlite3_threadsafe()); // whoa there big boy- gotta make sure sqlite it happy with what we're going to do.
    
    self = [super init];
    
    if (self) {
        _databasePath       = [aPath copy];
        _openResultSets     = [[NSMutableSet alloc] init];
        _db                 = 0x00;
        _logsErrors         = 0x00;
        _crashOnErrors      = 0x00;
        _busyRetryTimeout   = FMDB_BUSY_TIMEOUT;
        
        _dateFormatLock = [[NSObject alloc] init];
    }
    
    return self;
}

- (void)finalize {
    [self close];
    [super finalize];
}

- (void)dealloc {
    [self close];
    FMDBRelease(_openResultSets);
    FMDBRelease(_cachedStatements);
    FMDBRelease(_dateFormat);
    FMDBRelease(_databasePath);
    FMDBRelease(_openFunctions);
    FMDBRelease(_dateFormatLock);
    
#if ! __has_feature(objc_arc)
    [super dealloc];
#endif
}

- (NSString *)databasePath {
    return _databasePath;
}

- (sqlite3*)sqliteHandle {
    return _db;
}

- (const char*)sqlitePath {
    
    if (!_databasePath) {
        return ":memory:";
    }
    
    if ([_databasePath length] == 0) {
        return ""; // this creates a temporary database (it's an sqlite thing).
    }
    
    return [_databasePath fileSystemRepresentation];
    
}

- (BOOL)open {
    @synchronized (self) {
        if (_db) {
            return YES;
        }
        
        int err = sqlite3_open([self sqlitePath], &_db );
        if(err != SQLITE_OK) {
            FMDB_LOG_EF(@"error opening!: %d", err);
            return NO;
        }
        
        return YES;
    }
}

#if SQLITE_VERSION_NUMBER >= 3005000
- (BOOL)openWithFlags:(int)flags {
    @synchronized (self) {
        int err = sqlite3_open_v2([self sqlitePath], &_db, flags, NULL /* Name of VFS module to use */);
        if(err != SQLITE_OK) {
            FMDB_LOG_EF(@"error opening!: %d", err);
            return NO;
        }
        return YES;
    }
}
#endif






- (BOOL)_close:(BOOL *)retry {
    *retry = NO;
    
	@synchronized (self) {
    
        
        if (!_db) {
            return YES;
        }
        
        if (_openResultSets.count) {
            
            // close any result sets that are stale
            NSDate *now = [NSDate date];
            NSSet *openResultSets = [_openResultSets copy];
            for (NSValue *v in openResultSets) {
                FMResultSet *rs = (FMResultSet *)v.nonretainedObjectValue;
                NSTimeInterval staleness = [now timeIntervalSinceDate:rs.lastHit];
                if (staleness > FMDB_STALE_INTERVAL) {
                    FMDB_LOG_WF(@"Closing stale open result set: %@", rs.query);
                    [rs close];
                } else {
                    FMDB_LOG_WF(@"Can't close non-stale open result set (%f): %@", staleness, rs.query);
                }
            }
            
            if (_openResultSets.count) {
                // so long as we have tracked open result sets, we will retry, based on the assumption that our threads will soon finish with those sets.  This is a better assumption than "we will crash if there is an open statement" or "we will force close all open statements, leading to possible data corruption"
                *retry = YES;
            }
        }
        
        if (!*retry) {
            
            [self clearCachedStatements];
            
            int rc = sqlite3_close(_db);
                
            if (SQLITE_BUSY == rc || SQLITE_LOCKED == rc) {
                *retry = YES;
                
                // check for open statements that were somehow not registered with our framework...
                sqlite3_stmt *pStmt;
                while ((pStmt = sqlite3_next_stmt(_db, 0x00)) != 0) {
                    NSString  *sql = [NSString stringWithUTF8String:sqlite3_sql( pStmt )];
                    FMDB_LOG_WF(@"Closing leaked statement: %@", sql);
                    sqlite3_finalize(pStmt);
                    usleep(1);
                }
            }
            else if (SQLITE_OK != rc) {
                FMDB_LOG_EF(@"error closing %@!: %d", [self databasePath], rc);
            }
        }
        
        if (!*retry) {
            _db = nil;
        }
    }
    return !*retry;
}

- (BOOL)close {
    
    _closing = YES;
    
    BOOL retry = YES;
    NSDate *timeout = nil;
    while (retry) {
        BOOL result = [self _close:&retry];
        if (!retry)
            return result;
        
        if (!timeout)
            timeout = [NSDate dateWithTimeIntervalSinceNow:FMDB_CLOSE_TIMEOUT];
        
        if ([timeout compare:[NSDate date]] == NSOrderedAscending) {
            if (_openResultSets.count) {
                NSMutableString *result_sets = [[NSMutableString alloc] init];
                @synchronized (_openResultSets) {
                    for (NSValue *v in _openResultSets) {
                        FMResultSet *rs = (FMResultSet *)v.nonretainedObjectValue;
                        [result_sets appendString:@"\n"];
                        [result_sets appendString:rs.query];
                    }
                }
                
                FMDB_LOG_EF(@"Database busy (%@), unable to close, these result sets are still active: %@", [self databasePath], result_sets);
            } else {
                FMDB_LOG_EF(@"Database busy (%@), unable to close", [self databasePath]);
            }
            return NO;
        }
        
        FMDB_LOG_DF(@"Retrying close: %@", [self databasePath]);
        usleep(FMDB_RETRY_SLEEP_MICROSECONDS);
    }
    
    _closing = NO;
    
    return YES;
}

- (void)clearCachedStatements {
    
	@synchronized (self) {
        NSEnumerator *e = [_cachedStatements objectEnumerator];
        FMStatement *cachedStmt;
        
        while ((cachedStmt = [e nextObject])) {
            [cachedStmt close];
        }
        
        [_cachedStatements removeAllObjects];
    }
}

- (BOOL)hasOpenResultSets {
    return [_openResultSets count] > 0;
}

- (void)addOpenResultSet:(FMResultSet *)resultSet {
	@synchronized (_openResultSets) {
        [_openResultSets addObject:[NSValue valueWithNonretainedObject:resultSet]];
    }
}

- (void)resultSetDidClose:(FMResultSet *)resultSet {
	@synchronized (_openResultSets) {
        [_openResultSets removeObject:[NSValue valueWithNonretainedObject:resultSet]];
    }
}

- (FMStatement*)cachedStatementForQuery:(NSString*)query {
    return [_cachedStatements objectForKey:query];
}

- (void)setCachedStatement:(FMStatement*)statement forQuery:(NSString*)query {
    
	@synchronized (self) {
        query = [query copy]; // in case we got handed in a mutable string...
        
        [statement setQuery:query];
        
        [_cachedStatements setObject:statement forKey:query];
        
        FMDBRelease(query);
    }
}


- (BOOL)rekey:(NSString*)key {
#ifdef SQLITE_HAS_CODEC
    if (!key) {
        return NO;
    }
    
    int rc = sqlite3_rekey(_db, [key UTF8String], (int)strlen([key UTF8String]));
    
    if (rc != SQLITE_OK) {
        FMDB_LOG_EF(@"error on rekey: %d\n%@", rc, [self lastErrorMessage]);
    }
    
    return (rc == SQLITE_OK);
#else
    return NO;
#endif
}

- (BOOL)setKey:(NSString*)key {
#ifdef SQLITE_HAS_CODEC
    if (!key) {
        return NO;
    }
    
    int rc = sqlite3_key(_db, [key UTF8String], (int)strlen([key UTF8String]));
    
    return (rc == SQLITE_OK);
#else
    return NO;
#endif
}

+ (NSDateFormatter *)storeableDateFormat:(NSString *)format {
    
    NSDateFormatter *result = FMDBReturnAutoreleased([[NSDateFormatter alloc] init]);
    result.dateFormat = format;
    result.timeZone = [NSTimeZone timeZoneForSecondsFromGMT:0];
    result.locale = FMDBReturnAutoreleased([[NSLocale alloc] initWithLocaleIdentifier:@"en_US"]);
    return result;
}


- (BOOL)hasDateFormatter {
    return _dateFormat != nil;
}

- (void)setDateFormat:(NSDateFormatter *)format {
    @synchronized (_dateFormatLock) {
        FMDBAutorelease(_dateFormat);
        _dateFormat = FMDBReturnRetained(format);
    }
}

- (NSDate *)dateFromString:(NSString *)s {
    @synchronized (_dateFormatLock) {
        return [_dateFormat dateFromString:s];
    }
}

- (NSString *)stringFromDate:(NSDate *)date {
    @synchronized (_dateFormatLock) {
        return [_dateFormat stringFromDate:date];
    }
}


- (BOOL)goodConnection {
    
	@synchronized (self) {
        if (!_db) {
            return NO;
        }
        
        FMResultSet *rs = [self executeQuery:@"select name from sqlite_master where type='table'"];
        
        if (rs) {
            [rs close];
            return YES;
        }
    }
    
    return NO;
}

- (void)warnInUse {
    FMDB_LOG_WF(@"The FMDatabase %@ is currently in use.", self);
    
#ifndef NS_BLOCK_ASSERTIONS
    if (_crashOnErrors) {
        FMDB_LOG_AF(false, @"The FMDatabase %@ is currently in use.", self);
        abort();
    }
#endif
}

- (BOOL)databaseExists {
    
    if (!_db) {
            
        FMDB_LOG_WF(@"The FMDatabase %@ is not open.", self);
        
#ifndef NS_BLOCK_ASSERTIONS
        FMDB_LOG_AF(false, @"The FMDatabase %@ is not open.", self);
        if (_crashOnErrors) {
            abort();
        }
#endif
        
        return NO;
    }
    
    return YES;
}

- (NSString*)lastErrorMessage {
    return [NSString stringWithUTF8String:sqlite3_errmsg(_db)];
}

- (BOOL)hadError {
    int lastErrCode = [self lastErrorCode];
    
    return (lastErrCode > SQLITE_OK && lastErrCode < SQLITE_ROW);
}

- (int)lastErrorCode {
    return sqlite3_errcode(_db);
}


- (NSError*)errorWithMessage:(NSString*)message {
    NSDictionary* errorMessage = [NSDictionary dictionaryWithObject:message forKey:NSLocalizedDescriptionKey];
    
    return [NSError errorWithDomain:@"FMDatabase" code:sqlite3_errcode(_db) userInfo:errorMessage];    
}

- (NSError*)lastError {
   return [self errorWithMessage:[self lastErrorMessage]];
}

- (sqlite_int64)lastInsertRowId {
	@synchronized (self) {
        return sqlite3_last_insert_rowid(_db);
    }
}

- (int)changes {
	@synchronized (self) {
        return sqlite3_changes(_db);
    }
}

- (void)bindObject:(id)obj toColumn:(int)idx inStatement:(sqlite3_stmt*)pStmt {
    
    if ((!obj) || ((NSNull *)obj == [NSNull null])) {
        sqlite3_bind_null(pStmt, idx);
    }
    
    // FIXME - someday check the return codes on these binds.
    else if ([obj isKindOfClass:[NSData class]]) {
        const void *bytes = [obj bytes];
        if (!bytes) {
            // it's an empty NSData object, aka [NSData data].
            // Don't pass a NULL pointer, or sqlite will bind a SQL null instead of a blob.
            bytes = "";
        }
        sqlite3_bind_blob(pStmt, idx, bytes, (int)[obj length], SQLITE_STATIC);
    }
    else if ([obj isKindOfClass:[NSDate class]]) {
        if (self.hasDateFormatter)
            sqlite3_bind_text(pStmt, idx, [[self stringFromDate:obj] UTF8String], -1, SQLITE_STATIC);
        else
            sqlite3_bind_double(pStmt, idx, [obj timeIntervalSince1970]);
    }
    else if ([obj isKindOfClass:[NSDecimalNumber class]]) {
        sqlite3_bind_text(pStmt, idx, [[obj description] UTF8String], -1, SQLITE_STATIC);
    }
    else if ([obj isKindOfClass:[NSNumber class]]) {
        
        if (strcmp([obj objCType], @encode(BOOL)) == 0) {
            sqlite3_bind_int(pStmt, idx, ([obj boolValue] ? 1 : 0));
        }
        else if (strcmp([obj objCType], @encode(int)) == 0) {
            sqlite3_bind_int64(pStmt, idx, [obj longValue]);
        }
        else if (strcmp([obj objCType], @encode(long)) == 0) {
            sqlite3_bind_int64(pStmt, idx, [obj longValue]);
        }
        else if (strcmp([obj objCType], @encode(long long)) == 0) {
            sqlite3_bind_int64(pStmt, idx, [obj longLongValue]);
        }
        else if (strcmp([obj objCType], @encode(unsigned long long)) == 0) {
            sqlite3_bind_int64(pStmt, idx, (long long)[obj unsignedLongLongValue]);
        }
        else if (strcmp([obj objCType], @encode(float)) == 0) {
            sqlite3_bind_double(pStmt, idx, [obj floatValue]);
        }
        else if (strcmp([obj objCType], @encode(double)) == 0) {
            sqlite3_bind_double(pStmt, idx, [obj doubleValue]);
        }
        else {
            sqlite3_bind_text(pStmt, idx, [[obj description] UTF8String], -1, SQLITE_STATIC);
        }
    }
    else {
        sqlite3_bind_text(pStmt, idx, [[obj description] UTF8String], -1, SQLITE_STATIC);
    }
}

- (void)extractSQL:(NSString *)sql argumentsList:(va_list)args intoString:(NSMutableString *)cleanedSQL arguments:(NSMutableArray *)arguments {
    
    NSUInteger length = [sql length];
    unichar last = '\0';
    for (NSUInteger i = 0; i < length; ++i) {
        id arg = nil;
        unichar current = [sql characterAtIndex:i];
        unichar add = current;
        if (last == '%') {
            switch (current) {
                case '@':
                    arg = va_arg(args, id);
                    break;
                case 'c':
                    // warning: second argument to 'va_arg' is of promotable type 'char'; this va_arg has undefined behavior because arguments will be promoted to 'int'
                    arg = [NSString stringWithFormat:@"%c", va_arg(args, int)];
                    break;
                case 's':
                    arg = [NSString stringWithUTF8String:va_arg(args, char*)];
                    break;
                case 'd':
                case 'D':
                case 'i':
                    arg = [NSNumber numberWithInt:va_arg(args, int)];
                    break;
                case 'u':
                case 'U':
                    arg = [NSNumber numberWithUnsignedInt:va_arg(args, unsigned int)];
                    break;
                case 'h':
                    i++;
                    if (i < length && [sql characterAtIndex:i] == 'i') {
                        //  warning: second argument to 'va_arg' is of promotable type 'short'; this va_arg has undefined behavior because arguments will be promoted to 'int'
                        arg = [NSNumber numberWithShort:(short)(va_arg(args, int))];
                    }
                    else if (i < length && [sql characterAtIndex:i] == 'u') {
                        // warning: second argument to 'va_arg' is of promotable type 'unsigned short'; this va_arg has undefined behavior because arguments will be promoted to 'int'
                        arg = [NSNumber numberWithUnsignedShort:(unsigned short)(va_arg(args, uint))];
                    }
                    else {
                        i--;
                    }
                    break;
                case 'q':
                    i++;
                    if (i < length && [sql characterAtIndex:i] == 'i') {
                        arg = [NSNumber numberWithLongLong:va_arg(args, long long)];
                    }
                    else if (i < length && [sql characterAtIndex:i] == 'u') {
                        arg = [NSNumber numberWithUnsignedLongLong:va_arg(args, unsigned long long)];
                    }
                    else {
                        i--;
                    }
                    break;
                case 'f':
                    arg = [NSNumber numberWithDouble:va_arg(args, double)];
                    break;
                case 'g':
                    // warning: second argument to 'va_arg' is of promotable type 'float'; this va_arg has undefined behavior because arguments will be promoted to 'double'
                    arg = [NSNumber numberWithFloat:(float)(va_arg(args, double))];
                    break;
                case 'l':
                    i++;
                    if (i < length) {
                        unichar next = [sql characterAtIndex:i];
                        if (next == 'l') {
                            i++;
                            if (i < length && [sql characterAtIndex:i] == 'd') {
                                //%lld
                                arg = [NSNumber numberWithLongLong:va_arg(args, long long)];
                            }
                            else if (i < length && [sql characterAtIndex:i] == 'u') {
                                //%llu
                                arg = [NSNumber numberWithUnsignedLongLong:va_arg(args, unsigned long long)];
                            }
                            else {
                                i--;
                            }
                        }
                        else if (next == 'd') {
                            //%ld
                            arg = [NSNumber numberWithLong:va_arg(args, long)];
                        }
                        else if (next == 'u') {
                            //%lu
                            arg = [NSNumber numberWithUnsignedLong:va_arg(args, unsigned long)];
                        }
                        else {
                            i--;
                        }
                    }
                    else {
                        i--;
                    }
                    break;
                default:
                    // something else that we can't interpret. just pass it on through like normal
                    break;
            }
        }
        else if (current == '%') {
            // percent sign; skip this character
            add = '\0';
        }
        
        if (arg != nil) {
            [cleanedSQL appendString:@"?"];
            [arguments addObject:arg];
        }
        else if (add != '\0') {
            [cleanedSQL appendFormat:@"%C", add];
        }
        last = current;
    }
}

#ifdef DEBUG
- (NSString *)printObjectForLog:(NSObject *)obj {
    if (!obj)
        return  @"<null>";
    if ([obj isKindOfClass:[NSData class]])
        return [NSString stringWithFormat:@"Data: %d bytes", (int)[(NSData *)obj length]];
    return [obj description];
}
#endif

- (FMResultSet *)executeQuery:(NSString *)sql withParameterDictionary:(NSDictionary *)arguments {
    return [self executeQuery:sql withArgumentsInArray:nil orDictionary:arguments orVAList:nil];
}

- (FMResultSet *)executeQuery:(NSString *)sql withArgumentsInArray:(NSArray*)arrayArgs orDictionary:(NSDictionary *)dictionaryArgs orVAList:(va_list)args retry:(BOOL*)retry {
    
    *retry = NO;
    
	@synchronized (self) {

        if (![self databaseExists]) {
            return 0x00;
        }
        
        FMDB_LOG_T_UNITS start = FMDB_LOG_T_TIME();
        
        int rc                  = 0x00;
        sqlite3_stmt *pStmt     = 0x00;
        FMStatement *statement  = 0x00;
        FMResultSet *rs         = 0x00;
        
        if (_traceExecution && sql) {
            FMDB_LOG_IF(@"%@ executeQuery: %@", self, sql);
        }
        
        // there is a chance we won't use the cached statement even if we said we should
        BOOL cache_statement = _shouldCacheStatements;
        if (cache_statement) {
            statement = [self cachedStatementForQuery:sql];
            
            // if the statement is in use, we will generate a new statement, and won't cache it later
            if (statement.inUse) {
                statement = nil;
                cache_statement = NO;
            }
            
            pStmt = statement ? [statement statement] : 0x00;
        }
        
        if (!pStmt) {
            rc      = sqlite3_prepare_v2(_db, [sql UTF8String], -1, &pStmt, 0);
            
            if (SQLITE_BUSY == rc || SQLITE_LOCKED == rc) {
                *retry = YES;
                sqlite3_finalize(pStmt);
                _isExecutingStatement = NO;
            }
            else if (SQLITE_OK != rc) {
                
                if (_logsErrors) {
                    FMDB_LOG_EF(@"DB Error: %d \"%@\"\nDB Query: %@\nDB Path: %@", [self lastErrorCode], [self lastErrorMessage], sql, _databasePath);
#ifndef NS_BLOCK_ASSERTIONS
                    FMDB_LOG_AF(false, @"DB Error: %d \"%@\"", [self lastErrorCode], [self lastErrorMessage]);
                    if (_crashOnErrors) {
                        abort();
                    }
#endif
                }
                
                sqlite3_finalize(pStmt);
                _isExecutingStatement = NO;
                return nil;
            }
        }
        
        if (!*retry) {
            id obj;
            int idx = 0;
            int queryCount = sqlite3_bind_parameter_count(pStmt); // pointed out by Dominic Yu (thanks!)
#ifdef DEBUG
            NSMutableArray *logged_args = [NSMutableArray array];
#endif
            
            // If dictionaryArgs is passed in, that means we are using sqlite's named parameter support
            if (dictionaryArgs) {
                
                for (NSString *dictionaryKey in [dictionaryArgs allKeys]) {
                    
                    // Prefix the key with a colon.
                    NSString *parameterName = [[NSString alloc] initWithFormat:@":%@", dictionaryKey];
                    
                    // Get the index for the parameter name.
                    int namedIdx = sqlite3_bind_parameter_index(pStmt, [parameterName UTF8String]);
                    
                    FMDBRelease(parameterName);
                    
                    if (namedIdx > 0) {
                        // Standard binding from here.
                        [self bindObject:[dictionaryArgs objectForKey:dictionaryKey] toColumn:namedIdx inStatement:pStmt];
                        
#ifdef DEBUG
                        [logged_args addObject:[self printObjectForLog:obj]];
#endif
                        
                        // increment the binding count, so our check below works out
                        idx++;
                    }
                    else {
                        FMDB_LOG_VF(@"Could not find index for %@", dictionaryKey);
                    }
                }
            }
            else {
                    
                while (idx < queryCount) {
                    
                    if (arrayArgs) {
                        obj = [arrayArgs objectAtIndex:(NSUInteger)idx];
                    }
                    else {
                        obj = va_arg(args, id);
                    }
#ifdef DEBUG
                    [logged_args addObject:[self printObjectForLog:obj]];
#endif
                    
                    idx++;
                    
                    [self bindObject:obj toColumn:idx inStatement:pStmt];
                }
            }
            
            if (idx != queryCount) {
                FMDB_LOG_E(@"Error: the bind count is not correct for the # of variables");
                sqlite3_finalize(pStmt);
                _isExecutingStatement = NO;
                return nil;
            }
            
            FMDBRetain(statement); // to balance the release below
            
            if (!statement) {
                statement = [[FMStatement alloc] init];
                [statement setStatement:pStmt];
                
                if (cache_statement) {
                    [self setCachedStatement:statement forQuery:sql];
                }
            }
            
            // the statement gets closed in rs's dealloc or [rs close];
            rs = [FMResultSet resultSetWithStatement:statement usingParentDatabase:self];
            [rs setQuery:sql];
            
            [self addOpenResultSet:rs];
            
            [statement incUseCount];
            statement.inUse = YES;
            
            FMDBRelease(statement);
            
            
#ifdef DEBUG
            FMDB_LOG_VF( @"sql: %@, args: %@ t: %llu", sql, [logged_args componentsJoinedByString:@", "], FMDB_LOG_T_TIME() - start);
#else
            FMDB_LOG_TF_CUTOFF( QUERY_TIMING_CUTOFF, start, @"sql: %@", sql );
#endif
            return rs;
        }
    }
    
    return nil;
}


- (FMResultSet *)executeQuery:(NSString *)sql withArgumentsInArray:(NSArray*)arrayArgs orDictionary:(NSDictionary *)dictionaryArgs orVAList:(va_list)args {
    
    BOOL retry = YES;
    NSDate *timeout = nil; 
    while (retry) {
        if (_closing)
            return nil;
        
        FMResultSet *rs = [self executeQuery:sql withArgumentsInArray:arrayArgs orDictionary:dictionaryArgs orVAList:args retry:&retry];
        if (!retry)
            return rs;
        
        if (!timeout)
            timeout = [NSDate dateWithTimeIntervalSinceNow:_busyRetryTimeout];
        
        if ([timeout compare:[NSDate date]] == NSOrderedAscending) {
            FMDB_LOG_EF(@"Database busy (%@)", [self databasePath]);
            return nil;
        }
        
        usleep(FMDB_RETRY_SLEEP_MICROSECONDS);
    }
    return nil;
}

- (FMResultSet *)executeQuery:(NSString*)sql, ... {
    va_list args;
    va_start(args, sql);
    
    id result = [self executeQuery:sql withArgumentsInArray:nil orDictionary:nil orVAList:args];
    
    va_end(args);
    return result;
}

- (FMResultSet *)executeQueryWithFormat:(NSString*)format, ... {
    va_list args;
    va_start(args, format);
    
    NSMutableString *sql = [NSMutableString stringWithCapacity:[format length]];
    NSMutableArray *arguments = [NSMutableArray array];
    [self extractSQL:format argumentsList:args intoString:sql arguments:arguments];    
    
    va_end(args);
    
    return [self executeQuery:sql withArgumentsInArray:arguments];
}

- (FMResultSet *)executeQuery:(NSString *)sql withArgumentsInArray:(NSArray *)arguments {
    return [self executeQuery:sql withArgumentsInArray:arguments orDictionary:nil orVAList:nil];
}


- (BOOL)executeUpdate:(NSString*)sql error:(NSError**)outErr withArgumentsInArray:(NSArray*)arrayArgs orDictionary:(NSDictionary *)dictionaryArgs orVAList:(va_list)args retry:(BOOL*)retry
{
    *retry = NO;
    
	@synchronized (self) {
    
        if (![self databaseExists]) {
            return NO;
        }
        
        FMDB_LOG_T_UNITS start = FMDB_LOG_T_TIME();
        
        int rc                   = 0x00;
        sqlite3_stmt *pStmt      = 0x00;
        FMStatement *cachedStmt  = 0x00;
        
        if (_traceExecution && sql) {
            FMDB_LOG_IF(@"%@ executeUpdate: %@", self, sql);
        }
        
        if (_shouldCacheStatements) {
            cachedStmt = [self cachedStatementForQuery:sql];
            // our full exec is synced, no way for the statement to be in use here, unlike executeQuery
            pStmt = cachedStmt ? [cachedStmt statement] : 0x00;
        }
        
        if (!pStmt) {

            rc      = sqlite3_prepare_v2(_db, [sql UTF8String], -1, &pStmt, 0);
            if (SQLITE_BUSY == rc || SQLITE_LOCKED == rc) {
                *retry = YES;
                sqlite3_finalize(pStmt);
            }
            else if (SQLITE_OK != rc) {
                
                if (_logsErrors) {
                    FMDB_LOG_EF(@"DB Error: %d \"%@\"\nDB Query: %@\nDB Path: %@", [self lastErrorCode], [self lastErrorMessage], sql, _databasePath);
#ifndef NS_BLOCK_ASSERTIONS
                    FMDB_LOG_AF(false, @"DB Error: %d \"%@\"", [self lastErrorCode], [self lastErrorMessage]);
                    if (_crashOnErrors) {
                        abort();
                    }
#endif
                }
                
                sqlite3_finalize(pStmt);
                
                if (outErr) {
                    *outErr = [self errorWithMessage:[NSString stringWithUTF8String:sqlite3_errmsg(_db)]];
                }
                
                return NO;
            }
        }
        
        if (!*retry) {
            id obj;
            int idx = 0;
            int queryCount = sqlite3_bind_parameter_count(pStmt);
#ifdef DEBUG
            NSMutableArray *logged_args = [NSMutableArray array];
#endif
            
            // If dictionaryArgs is passed in, that means we are using sqlite's named parameter support
            if (dictionaryArgs) {
                
                for (NSString *dictionaryKey in [dictionaryArgs allKeys]) {
                    
                    // Prefix the key with a colon.
                    NSString *parameterName = [[NSString alloc] initWithFormat:@":%@", dictionaryKey];
                    
                    // Get the index for the parameter name.
                    int namedIdx = sqlite3_bind_parameter_index(pStmt, [parameterName UTF8String]);
                    
                    FMDBRelease(parameterName);
                    
                    if (namedIdx > 0) {
                        // Standard binding from here.
                        [self bindObject:[dictionaryArgs objectForKey:dictionaryKey] toColumn:namedIdx inStatement:pStmt];
#ifdef DEBUG
                        [logged_args addObject:[self printObjectForLog:obj]];
#endif
                        
                        // increment the binding count, so our check below works out
                        idx++;
                    }
                    else {
                        FMDB_LOG_VF(@"Could not find index for %@", dictionaryKey);
                    }
                }
            }
            else {
                
                while (idx < queryCount) {
                    
                    if (arrayArgs) {
                        obj = [arrayArgs objectAtIndex:(NSUInteger)idx];
                    }
                    else {
                        obj = va_arg(args, id);
                    }
                    
#ifdef DEBUG
                    [logged_args addObject:[self printObjectForLog:obj]];
#endif
                    
                    idx++;
                    
                    [self bindObject:obj toColumn:idx inStatement:pStmt];
                }
            }
            
            
            if (idx != queryCount) {
                FMDB_LOG_EF(@"Error: the bind count (%d) is not correct for the # of variables in the query (%d) (%@) (executeUpdate)", idx, queryCount, sql);
                if (cachedStmt)
                    [cachedStmt reset];
                else
                    sqlite3_finalize(pStmt);
                return NO;
            }
        
            /* Call sqlite3_step() to run the virtual machine. Since the SQL being
             ** executed is not a SELECT statement, we assume no data will be returned.
             */
            rc      = sqlite3_step(pStmt);
                
            if (SQLITE_BUSY == rc || SQLITE_LOCKED == rc) {
                // this will happen if the db is locked, like if we are doing an update or insert.
                // in that case, retry the step... and maybe wait just 10 milliseconds.
                *retry = YES;
                if (cachedStmt) {
                    [cachedStmt reset];
                } else {
                    sqlite3_finalize(pStmt);
                }
            }
            else if (SQLITE_DONE == rc) {
                // all is well, let's return.
            }
            else if (SQLITE_ERROR == rc) {
                FMDB_LOG_EF(@"Error calling sqlite3_step (%d: %s) SQLITE_ERROR\nDB Query: %@", rc, sqlite3_errmsg(_db), sql);
            }
            else if (SQLITE_MISUSE == rc) {
                // uh oh.
                FMDB_LOG_EF(@"Error calling sqlite3_step (%d: %s) SQLITE_MISUSE\nDB Query: %@", rc, sqlite3_errmsg(_db), sql);
            }
            else {
                // wtf?
                FMDB_LOG_EF(@"Unknown error calling sqlite3_step (%d: %s) eu\nDB Query: %@", rc, sqlite3_errmsg(_db), sql);
            }
            
            if (!*retry) {
                if (rc == SQLITE_ROW) {
                    FMDB_LOG_AF(NO, @"A executeUpdate is being called with a query string '%@'", sql);
                }
                
                if (_shouldCacheStatements && !cachedStmt) {
                    cachedStmt = [[FMStatement alloc] init];
                    
                    [cachedStmt setStatement:pStmt];
                    
                    [self setCachedStatement:cachedStmt forQuery:sql];
                    
                    FMDBRelease(cachedStmt);
                }
                
                int closeErrorCode;
                
                if (cachedStmt) {
                    [cachedStmt incUseCount];
                    closeErrorCode = sqlite3_reset(pStmt);
                }
                else {
                    /* Finalize the virtual machine. This releases all memory and other
                     ** resources allocated by the sqlite3_prepare() call above.
                     */
                    closeErrorCode = sqlite3_finalize(pStmt);
                }
                
                if (closeErrorCode != SQLITE_OK) {
                    FMDB_LOG_EF(@"Unknown error finalizing or resetting statement (%d: %s)\nDB Query: %@", closeErrorCode, sqlite3_errmsg(_db), sql);
                }
                
#ifdef DEBUG
                FMDB_LOG_VF( @"sql: %@, args: %@ t: %llu", sql, [logged_args componentsJoinedByString:@", "], FMDB_LOG_T_TIME() - start);
#else
                FMDB_LOG_TF_CUTOFF( QUERY_TIMING_CUTOFF, start, @"sql: %@", sql );
#endif
                
                return (rc == SQLITE_DONE || rc == SQLITE_OK);
            }
        }
    }
    
    return NO;
}


- (BOOL)executeUpdate:(NSString*)sql error:(NSError**)outErr withArgumentsInArray:(NSArray*)arrayArgs orDictionary:(NSDictionary *)dictionaryArgs orVAList:(va_list)args
{
    BOOL retry = YES;
    NSDate *timeout = nil;
    while (retry) {
        if (_closing)
            return NO;
        
        BOOL result = [self executeUpdate:sql error:outErr withArgumentsInArray:arrayArgs orDictionary:dictionaryArgs orVAList:args retry:&retry];
        if (!retry)
            return result;
        
        if (!timeout)
            timeout = [NSDate dateWithTimeIntervalSinceNow:_busyRetryTimeout];
        
        if ([timeout compare:[NSDate date]] == NSOrderedAscending) {
            FMDB_LOG_EF(@"Database busy (%@)", [self databasePath]);
            return NO;
        }
        
        usleep(FMDB_RETRY_SLEEP_MICROSECONDS);
    }
    return NO;
}


- (BOOL)executeUpdate:(NSString*)sql, ... {
    va_list args;
    va_start(args, sql);
    
    BOOL result = [self executeUpdate:sql error:nil withArgumentsInArray:nil orDictionary:nil orVAList:args];
    
    va_end(args);
    return result;
}

- (BOOL)executeUpdate:(NSString*)sql withArgumentsInArray:(NSArray *)arguments {
    return [self executeUpdate:sql error:nil withArgumentsInArray:arguments orDictionary:nil orVAList:nil];
}

- (BOOL)executeUpdate:(NSString*)sql withParameterDictionary:(NSDictionary *)arguments {
    return [self executeUpdate:sql error:nil withArgumentsInArray:nil orDictionary:arguments orVAList:nil];
}

- (BOOL)executeUpdateWithFormat:(NSString*)format, ... {
    va_list args;
    va_start(args, format);
    
    NSMutableString *sql      = [NSMutableString stringWithCapacity:[format length]];
    NSMutableArray *arguments = [NSMutableArray array];
    
    [self extractSQL:format argumentsList:args intoString:sql arguments:arguments];    
    
    va_end(args);
    
    return [self executeUpdate:sql withArgumentsInArray:arguments];
}

- (BOOL)update:(NSString*)sql withErrorAndBindings:(NSError**)outErr, ... {
    va_list args;
    va_start(args, outErr);
    
    BOOL result = [self executeUpdate:sql error:outErr withArgumentsInArray:nil orDictionary:nil orVAList:args];
    
    va_end(args);
    return result;
}

- (BOOL)rollback {
    BOOL b = [self executeUpdate:@"rollback transaction"];
    
    if (b) {
        _inTransaction = NO;
    }
    
    return b;
}

- (BOOL)commit {
    BOOL b =  [self executeUpdate:@"commit transaction"];
    
    if (b) {
        _inTransaction = NO;
    }
    
    return b;
}

- (BOOL)beginDeferredTransaction {
    
    BOOL b = [self executeUpdate:@"begin deferred transaction"];
    if (b) {
        _inTransaction = YES;
    }
    
    return b;
}

- (BOOL)beginTransaction {
    
    BOOL b = [self executeUpdate:@"begin exclusive transaction"];
    if (b) {
        _inTransaction = YES;
    }
    
    return b;
}

- (BOOL)inTransaction {
    return _inTransaction;
}

#if SQLITE_VERSION_NUMBER >= 3007000

- (BOOL)startSavePointWithName:(NSString*)name error:(NSError**)outErr {
    
    // FIXME: make sure the savepoint name doesn't have a ' in it.
    
    NSParameterAssert(name);
    
    if (![self executeUpdate:[NSString stringWithFormat:@"savepoint '%@';", name]]) {

        if (outErr) {
            *outErr = [self lastError];
        }
        
        return NO;
    }
    
    return YES;
}

- (BOOL)releaseSavePointWithName:(NSString*)name error:(NSError**)outErr {
    
    NSParameterAssert(name);
    
    BOOL worked = [self executeUpdate:[NSString stringWithFormat:@"release savepoint '%@';", name]];
    
    if (!worked && outErr) {
        *outErr = [self lastError];
    }
    
    return worked;
}

- (BOOL)rollbackToSavePointWithName:(NSString*)name error:(NSError**)outErr {
    
    NSParameterAssert(name);
    
    BOOL worked = [self executeUpdate:[NSString stringWithFormat:@"rollback transaction to savepoint '%@';", name]];
    
    if (!worked && *outErr) {
        *outErr = [self lastError];
    }
    
    return worked;
}

- (NSError*)inSavePoint:(void (^)(BOOL *rollback))block {
    static unsigned long savePointIdx = 0;
    
    NSString *name = [NSString stringWithFormat:@"dbSavePoint%ld", savePointIdx++];
    
    BOOL shouldRollback = NO;
    
    NSError *err = 0x00;
    
    if (![self startSavePointWithName:name error:&err]) {
        return err;
    }
    
    block(&shouldRollback);
    
    if (shouldRollback) {
        [self rollbackToSavePointWithName:name error:&err];
    }
    else {
        [self releaseSavePointWithName:name error:&err];
    }
    
    return err;
}

#endif


- (BOOL)shouldCacheStatements {
    return _shouldCacheStatements;
}

- (void)setShouldCacheStatements:(BOOL)value {
    
    _shouldCacheStatements = value;
    
    if (_shouldCacheStatements && !_cachedStatements) {
        [self setCachedStatements:[NSMutableDictionary dictionary]];
    }
    
    if (!_shouldCacheStatements) {
        [self setCachedStatements:nil];
    }
}

void FMDBBlockSQLiteCallBackFunction(sqlite3_context *context, int argc, sqlite3_value **argv);
void FMDBBlockSQLiteCallBackFunction(sqlite3_context *context, int argc, sqlite3_value **argv) {
#if ! __has_feature(objc_arc)
    void (^block)(sqlite3_context *context, int argc, sqlite3_value **argv) = (id)sqlite3_user_data(context);
#else
    void (^block)(sqlite3_context *context, int argc, sqlite3_value **argv) = (__bridge id)sqlite3_user_data(context);
#endif
    block(context, argc, argv);
}


- (void)makeFunctionNamed:(NSString*)name maximumArguments:(int)count withBlock:(void (^)(sqlite3_context *context, int argc, sqlite3_value **argv))block {
    
    if (!_openFunctions) {
        _openFunctions = [NSMutableSet new];
    }
    
    id b = FMDBReturnAutoreleased([block copy]);
    
    [_openFunctions addObject:b];
    
    /* I tried adding custom functions to release the block when the connection is destroyed- but they seemed to never be called, so we use _openFunctions to store the values instead. */
#if ! __has_feature(objc_arc)
    sqlite3_create_function([self sqliteHandle], [name UTF8String], count, SQLITE_UTF8, (void*)b, &FMDBBlockSQLiteCallBackFunction, 0x00, 0x00);
#else
    sqlite3_create_function([self sqliteHandle], [name UTF8String], count, SQLITE_UTF8, (__bridge void*)b, &FMDBBlockSQLiteCallBackFunction, 0x00, 0x00);
#endif
}

@end



@implementation FMStatement
@synthesize statement=_statement;
@synthesize query=_query;
@synthesize useCount=_useCount;
@synthesize inUse=_inUse;

- (void)finalize {
    [self close];
    [super finalize];
}

- (void)dealloc {
    [self close];
    FMDBRelease(_query);
#if ! __has_feature(objc_arc)
    [super dealloc];
#endif
}

- (void)close {
    if (_statement) {
        sqlite3_finalize(_statement);
        _statement = 0x00;
    }
}

- (void)reset {
    if (_statement) {
        sqlite3_reset(_statement);
        self.inUse = NO;
    }
}

- (NSString*)description {
    return [NSString stringWithFormat:@"%@ %@ %ld hit(s) for query %@", [super description], _inUse ? @"In Use" : @"Not In Use", _useCount, _query];
}

- (void)incUseCount {
    @synchronized (self) {
        ++_useCount;
    }
}


@end

