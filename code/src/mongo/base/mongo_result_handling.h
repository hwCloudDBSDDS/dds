
#pragma once

#include "status.h"
#include "status_with.h"

//
//  If given operation failed, return its error code, plus
//  log error message. Operations always return MongoDB Status.
//
#define MDB_RIF(op)                                                             \
    {                                                                           \
        auto ____result = (op);                                                 \
        if(!ResultHandling::IsOK(____result))                                   \
        {                                                                       \
            return ResultHandling::LogErrorReturn(____result, #op, __FILE__, __LINE__, __FUNCTION__);    \
        }                                                                       \
    } 

//
//  Return result of the given operation.
//  If result is failure - log it.
//
#define MDB_RET(op)                                                             \
    {                                                                           \
        auto ____result = ResultHandling::MongoConvertStatus(op);               \
        if(!ResultHandling::IsOK(____result))                                   \
        {                                                                       \
            ResultHandling::LogError(____result, #op, __FILE__, __LINE__, __FUNCTION__);         \
        }                                                                       \
        return ____result;                                                      \
    }

#define MDB_RIF_W(result, op)                                                   \
    {                                                                           \
        auto ____result = (op);                                                 \
        if(!ResultHandling::IsOK(____result))                                   \
        {                                                                       \
            return ResultHandling::LogErrorReturn(____result, #op, __FILE__, __LINE__, __FUNCTION__);    \
        }                                                                       \
        result = std::move(statusWithResult.getValue())                         \
    } 

namespace rocksdb
{
class Status;
}


namespace ResultHandling
{

inline bool IsOK(const mongo::Status& status)
{
    return status.isOK();
}

inline std::string ToString(const mongo::Status& status)
{
    return status.toString();
}


template<typename T>
inline bool IsOK(const mongo::StatusWith<T>& status)
{
    return status.isOK();
}

template<typename T>
inline std::string ToString(const mongo::StatusWith<T>& status)
{
    return status.getStatus().toString();
}


inline mongo::Status MongoConvertStatus(mongo::Status status)
{
    return status;
}

inline mongo::Status& MongoConvertStatus(mongo::Status& status)
{
    return status;
}

template<typename T>
inline mongo::Status& MongoConvertStatus(mongo::StatusWith<T>& status)
{
    return status.getStatus();
}


void LogError(
    const mongo::Status& result,
    const char* operationText,
    const char* file,
    int line,
    const char* function);


mongo::Status MongoConvertStatus(const rocksdb::Status& status);

template<typename StatusT>
inline mongo::Status LogErrorReturn(
    StatusT& result,
    const char* operationText,
    const char* file,
    int line,
    const char* function) //-> decltype(MongoConvertStatus(result))
{
    auto status = MongoConvertStatus(result);
    LogError(status, operationText, file, line, function);
    return status;
}

inline mongo::Status& LogErrorReturn(
    mongo::Status& result,
    const char* operationText,
    const char* file,
    int line,
    const char* function)
{
    LogError(result, operationText, file, line, function);
    return result;
}

}  // ResultHandling
