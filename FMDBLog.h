//
//  FMDBLog.h
//  fmdb
//
//  Created by Chris Wright on 13-02-20.
//
//

// Stub macros for improved logging.  See un1v3rse/U13Log for an implementation that fits nicely with this.  If U13Log is in the prefix file, LOG_E will already be defined.

#ifdef LOG_E

#define FMDB_LOG_E LOG_E
#define FMDB_LOG_EF LOG_EF
#define FMDB_LOG_W LOG_W
#define FMDB_LOG_WF LOG_WF
#define FMDB_LOG_I LOG_I
#define FMDB_LOG_IF LOG_IF

#define FMDB_LOG_D LOG_D
#define FMDB_LOG_DF LOG_DF
#define FMDB_LOG_V LOG_V
#define FMDB_LOG_VF LOG_VF
#define FMDB_LOG_A LOG_A
#define FMDB_LOG_AF LOG_AF

#define FMDB_LOG_T LOG_T
#define FMDB_LOG_TF LOG_TF
#define FMDB_LOG_T_CUTOFF LOG_T_CUTOFF
#define FMDB_LOG_TF_CUTOFF LOG_TF_CUTOFF

#else

#define FMDB_LOG(level,msg) NSLog(@"%s:%d %@ %@", __PRETTY_FUNCTION__, __LINE__, level, msg)
#define FMDB_LOGF(level,fmt,...) NSLog(@"%s:%d %@ %@", __PRETTY_FUNCTION__, __LINE__, level, [NSString stringWithFormat:fmt, __VA_ARGS__])

#define FMDB_LOG_E(msg) FMDB_LOG(@"E",msg)
#define FMDB_LOG_EF(fmt,...) FMDB_LOGF(@"E",fmt,__VA_ARGS__)
#define FMDB_LOG_W(msg) FMDB_LOG(@"W",msg)
#define FMDB_LOG_WF(fmt,...) FMDB_LOGF(@"W",fmt,__VA_ARGS__)
#define FMDB_LOG_I(msg) FMDB_LOG(@"I",msg)
#define FMDB_LOG_IF(fmt,...) FMDB_LOGF(@"I",fmt,__VA_ARGS__)

#ifdef DEBUG
#define FMDB_LOG_D(msg) FMDB_LOG(@"D",msg)
#define FMDB_LOG_DF(fmt,...) FMDB_LOGF(@"D",fmt,__VA_ARGS__)
#define FMDB_LOG_V(msg) FMDB_LOG(@"V",msg)
#define FMDB_LOG_VF(fmt,...) FMDB_LOGF(@"V",fmt,__VA_ARGS__)
#define FMDB_LOG_A(condition, msg) if (!(condition)) FMDB_LOG_E(msg)
#define FMDB_LOG_AF(condition, fmt,...) if (!(condition)) FMDB_LOG_E(fmt,__VA_ARGS__)
#else
#define FMDB_LOG_D(msg)
#define FMDB_LOG_DF(fmt,...)
#define FMDB_LOG_V(msg)
#define FMDB_LOG_VF(fmt,...)
#define FMDB_LOG_A(condition, msg)
#define FMDB_LOG_AF(condition, fmt,...)
#endif

#define FMDB_LOG_T(start,msg)
#define FMDB_LOG_TF(start,fmt,...)
#define FMDB_LOG_T_CUTOFF(cutoff,start,msg)
#define FMDB_LOG_TF_CUTOFF(cutoff,start,fmt,...)

#endif

