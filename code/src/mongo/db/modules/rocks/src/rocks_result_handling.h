
#pragma once

#include <rocksdb/status.h>
#include <rocksdb/env.h>


//
//  If given operation failed, return its error status from the function that
//  calls this macros. Log error message.
//
#define RDB_RIF(op) RES_RIF(op)


//
//  Return result of the given operation.
//  If result is failure - log it.
//
#define RDB_RET(op) RES_RET(op)



namespace ResultHandling
{

inline bool IsOK(const rocksdb::Status& status)
{
    return status.ok();
}

inline std::string ToString(const rocksdb::Status& status)
{
    return status.ToString();
}

class RocksDBLogging
{
    static rocksdb::Logger* logger;
public:
    static void SetLogger(rocksdb::Logger* l)
    {
        logger = l;
    }

    static rocksdb::Logger* Logger()
    {
        return logger; 
    }
};
#if 1
void LogError(
    const rocksdb::Status& result,
    const char* operationText,
    const char* file,
    int line,
    const char* function);
#endif

}  // ResultHandling




