//
//  FMDatabaseAdditions.m
//  fmkit
//
//  Created by August Mueller on 10/30/05.
//  Copyright 2005 Flying Meat Inc.. All rights reserved.
//

#import "FMDatabase.h"
#import "FMDatabaseAdditions.h"
#import "FMDBLog.h"

@interface FMDatabase (PrivateStuff)
- (FMResultSet *)executeQuery:(NSString *)sql withArgumentsInArray:(NSArray*)arrayArgs orDictionary:(NSDictionary *)dictionaryArgs orVAList:(va_list)args;
@end

@implementation FMDatabase (FMDatabaseAdditions)

- (NSString*)stringForQuery:(NSString*)query, ... {
    NSString *result = nil;
    va_list args;
    va_start(args, query);
    FMResultSet *rs = [self executeQuery:query withArgumentsInArray:nil orDictionary:nil orVAList:args];
    va_end(args);
    if ([rs next]) {
        result = [rs stringForColumnIndex:0];
    }
    [rs close];
    return result;
}

- (int)intForQuery:(NSString*)query, ... {
	int result = 0;
    va_list args;
    va_start(args, query);
    
    FMResultSet* rs = [self executeQuery:query withArgumentsInArray:nil orDictionary:nil orVAList:args];
	if ([rs next]) {
		result = [rs intForColumnIndex:0];
	}
	[rs close];
    
    va_end(args);
    return result;
}

- (long)longForQuery:(NSString*)query, ... {
	long result = 0L;
    va_list args;
    va_start(args, query);
    
    FMResultSet* rs = [self executeQuery:query withArgumentsInArray:nil orDictionary:nil orVAList:args];
	if ([rs next]) {
		result = [rs longForColumnIndex:0];
	}
	[rs close];
    
    va_end(args);
    return result;
}

- (long long int)longLongForQuery:(NSString*)query, ... {
	long long int result = 0LL;
    va_list args;
    va_start(args, query);
    
    FMResultSet* rs = [self executeQuery:query withArgumentsInArray:nil orDictionary:nil orVAList:args];
	if ([rs next]) {
		result = [rs longLongIntForColumnIndex:0];
	}
	[rs close];
    
    va_end(args);
    return result;
}

- (BOOL)boolForQuery:(NSString*)query, ... {
	BOOL result = NO;
    va_list args;
    va_start(args, query);
    
    FMResultSet* rs = [self executeQuery:query withArgumentsInArray:nil orDictionary:nil orVAList:args];
	if ([rs next]) {
		result = [rs boolForColumnIndex:0];
	}
	[rs close];
    
    va_end(args);
    return result;
}

- (double)doubleForQuery:(NSString*)query, ... {
	double result = 0;
    va_list args;
    va_start(args, query);
    
    FMResultSet* rs = [self executeQuery:query withArgumentsInArray:nil orDictionary:nil orVAList:args];
	if ([rs next]) {
		result = [rs doubleForColumnIndex:0];
	}
	[rs close];
    
    va_end(args);
    return result;
}

- (NSData*)dataForQuery:(NSString*)query, ... {
    NSData *result = nil;
    va_list args;
    va_start(args, query);
    FMResultSet *rs = [self executeQuery:query withArgumentsInArray:nil orDictionary:nil orVAList:args];
    va_end(args);
    if ([rs next]) {
        result = [rs dataForColumnIndex:0];
    }
    [rs close];
    return result;
}

- (NSDate*)dateForQuery:(NSString*)query, ... {
    NSDate *result = nil;
    va_list args;
    va_start(args, query);
    FMResultSet *rs = [self executeQuery:query withArgumentsInArray:nil orDictionary:nil orVAList:args];
    va_end(args);
    if ([rs next]) {
        result = [rs dateForColumnIndex:0];
    }
    [rs close];
    return result;
}

- (NSNumber*)numberForQuery:(NSString*)query, ... {
    NSNumber *result = nil;
    va_list args;
    va_start(args, query);
    FMResultSet *rs = [self executeQuery:query withArgumentsInArray:nil orDictionary:nil orVAList:args];
    va_end(args);
    if ([rs next]) {
        result = [rs numberForColumnIndex:0];
    }
    [rs close];
    return result;
}

- (NSDecimalNumber*)decimalNumberForQuery:(NSString*)query, ... {
    NSDecimalNumber *result = nil;
    va_list args;
    va_start(args, query);
    FMResultSet *rs = [self executeQuery:query withArgumentsInArray:nil orDictionary:nil orVAList:args];
    va_end(args);
    if ([rs next]) {
        result = [rs decimalNumberForColumnIndex:0];
    }
    [rs close];
    return result;
}


- (BOOL)tableExists:(NSString*)tableName {
    
    tableName = [tableName lowercaseString];
    
    FMResultSet *rs = [self executeQuery:@"select [sql] from sqlite_master where [type] = 'table' and lower(name) = ?", tableName];
    
    //if at least one next exists, table exists
    BOOL returnBool = [rs next];
    
    //close and free object
    [rs close];
    
    return returnBool;
}

/*
 get table with list of tables: result colums: type[STRING], name[STRING],tbl_name[STRING],rootpage[INTEGER],sql[STRING]
 check if table exist in database  (patch from OZLB)
*/
- (FMResultSet*)getSchema {
    
    //result colums: type[STRING], name[STRING],tbl_name[STRING],rootpage[INTEGER],sql[STRING]
    FMResultSet *rs = [self executeQuery:@"SELECT type, name, tbl_name, rootpage, sql FROM (SELECT * FROM sqlite_master UNION ALL SELECT * FROM sqlite_temp_master) WHERE type != 'meta' AND name NOT LIKE 'sqlite_%' ORDER BY tbl_name, type DESC, name"];
    
    return rs;
}

/* 
 get table schema: result colums: cid[INTEGER], name,type [STRING], notnull[INTEGER], dflt_value[],pk[INTEGER]
*/
- (FMResultSet*)getTableSchema:(NSString*)tableName {
    
    //result colums: cid[INTEGER], name,type [STRING], notnull[INTEGER], dflt_value[],pk[INTEGER]
    FMResultSet *rs = [self executeQuery:[NSString stringWithFormat: @"PRAGMA table_info('%@')", tableName]];
    
    return rs;
}

- (BOOL)columnExists:(NSString*)columnName inTableWithName:(NSString*)tableName {
    
    BOOL returnBool = NO;
    
    tableName  = [tableName lowercaseString];
    columnName = [columnName lowercaseString];
    
    FMResultSet *rs = [self getTableSchema:tableName];
    
    //check if column is present in table schema
    while ([rs next]) {
        if ([[[rs stringForColumn:@"name"] lowercaseString] isEqualToString:columnName]) {
            returnBool = YES;
            break;
        }
    }
    
    //If this is not done FMDatabase instance stays out of pool
    [rs close];
    
    return returnBool;
}

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-implementations"

- (BOOL)columnExists:(NSString*)tableName columnName:(NSString*)columnName __attribute__ ((deprecated)) {
    return [self columnExists:columnName inTableWithName:tableName];
}

#pragma clang diagnostic pop

- (BOOL)validateSQL:(NSString*)sql error:(NSError**)error retry:(BOOL *)retry {
    sqlite3_stmt *pStmt = NULL;
    BOOL validationSucceeded = YES;
    *retry = NO;
    
    @synchronized (self) {
        int rc = sqlite3_prepare_v2(_db, [sql UTF8String], -1, &pStmt, 0);
        if (rc == SQLITE_BUSY || rc == SQLITE_LOCKED) {
            *retry = YES;
            validationSucceeded = NO;
        } 
        else if (rc != SQLITE_OK) {
            validationSucceeded = NO;
            if (error) {
                *error = [NSError errorWithDomain:NSCocoaErrorDomain 
                                             code:[self lastErrorCode]
                                         userInfo:[NSDictionary dictionaryWithObject:[self lastErrorMessage] 
                                                                              forKey:NSLocalizedDescriptionKey]];
            }
        }
    }
    
    sqlite3_finalize(pStmt);
    
    return validationSucceeded;
}

- (BOOL)validateSQL:(NSString*)sql error:(NSError**)error {
    
    BOOL retry = YES;
    NSDate *timeout = nil;
    while (retry) {
        if (_closing)
            return NO;
        
        BOOL result = [self validateSQL:sql error:error retry:&retry];
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

@end
