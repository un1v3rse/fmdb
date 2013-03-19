//
//  FMDBLog.h
//  fmdb
//
//  Created by Chris Wright on 13-02-20.
//
//

// Stub macros for improved logging.  See un1v3rse/U13Log for an implementation that fits nicely with this.  If U13Log is in the prefix file, LOG_E will already be defined.

#ifdef LOG_E

#define FMDB_LOG_SET_LEVEL LOG_SET_LEVEL
#define FMDB_LOG_SET_DEBUG_BREAK_ENABLED LOG_SET_DEBUG_BREAK_ENABLED

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

#define FMDB_LOG_T_UNITS LOG_T_UNITS
#define FMDB_LOG_T_TIME LOG_T_TIME
#define FMDB_LOG_T LOG_T
#define FMDB_LOG_TF LOG_TF
#define FMDB_LOG_T_CUTOFF LOG_T_CUTOFF
#define FMDB_LOG_TF_CUTOFF LOG_TF_CUTOFF

#else

#include <mach/mach_time.h>

enum {
	LOG_LEVEL_VERBOSE = 0,
	LOG_LEVEL_DEBUG,
	LOG_LEVEL_INFO,
	LOG_LEVEL_PERFORMANCE,
	LOG_LEVEL_WARNING,
	LOG_LEVEL_ERROR,
    LOG_LEVEL_COUNT,
};

static int FMDB_LOG_LEVEL = LOG_LEVEL_DEBUG;

#define FMDB_LOG_SET_DEBUG_BREAK_ENABLED(enabled)

#define FMDB_LOG_SET_LEVEL(level) FMDB_LOG_LEVEL = level

#define FMDB_LOG(level,msg) if (level >= FMDB_LOG_LEVEL) { NSLog(@"%s:%d %@", __PRETTY_FUNCTION__, __LINE__, msg); } else
#define FMDB_LOGF(level,fmt,...) if (level >= FMDB_LOG_LEVEL) { NSLog(@"%s:%d %@", __PRETTY_FUNCTION__, __LINE__, [NSString stringWithFormat:fmt, __VA_ARGS__]); } else

#define FMDB_LOG_E(msg) FMDB_LOG(LOG_LEVEL_ERROR,msg)
#define FMDB_LOG_EF(fmt,...) FMDB_LOGF(LOG_LEVEL_ERROR,fmt,__VA_ARGS__)
#define FMDB_LOG_W(msg) FMDB_LOG(LOG_LEVEL_WARNING,msg)
#define FMDB_LOG_WF(fmt,...) FMDB_LOGF(LOG_LEVEL_WARNING,fmt,__VA_ARGS__)
#define FMDB_LOG_I(msg) FMDB_LOG(LOG_LEVEL_INFO,msg)
#define FMDB_LOG_IF(fmt,...) FMDB_LOGF(LOG_LEVEL_INFO,fmt,__VA_ARGS__)

#ifdef DEBUG
#define FMDB_LOG_D(msg) FMDB_LOG(LOG_LEVEL_DEBUG,msg)
#define FMDB_LOG_DF(fmt,...) FMDB_LOGF(LOG_LEVEL_DEBUG,fmt,__VA_ARGS__)
#define FMDB_LOG_V(msg) FMDB_LOG(LOG_LEVEL_VERBOSE,msg)
#define FMDB_LOG_VF(fmt,...) FMDB_LOGF(LOG_LEVEL_VERBOSE,fmt,__VA_ARGS__)
#define FMDB_LOG_A(condition, msg) if (!(condition)) { FMDB_LOG_E(msg) ;}
#define FMDB_LOG_AF(condition, fmt,...) if (!(condition)) { FMDB_LOG_EF(fmt,__VA_ARGS__); }
#else
#define FMDB_LOG_D(msg)
#define FMDB_LOG_DF(fmt,...)
#define FMDB_LOG_V(msg)
#define FMDB_LOG_VF(fmt,...)
#define FMDB_LOG_A(condition, msg)
#define FMDB_LOG_AF(condition, fmt,...)
#endif

typedef uint64_t FMDB_LOG_T_UNITS;
#define FMDB_LOG_T_TIME() mach_absolute_time()
#define FMDB_LOG_T(start,msg)
#define FMDB_LOG_TF(start,fmt,...)
#define FMDB_LOG_T_CUTOFF(cutoff,start,msg)
#define FMDB_LOG_TF_CUTOFF(cutoff,start,fmt,...)

#endif

