
#pragma once

#include <stdio.h>
#include <stdlib.h>

#define RESULT_HANDLING_GENERIC_RIF(op, ConvertResultFunc, LogErrorFunc, ProccessResultFunc) \
    {                                                                                        \
        auto ____result = (op);                                                              \
        if (!ResultHandling::IsOK(____result)) {                                             \
            LogErrorFunc(____result, #op, __FILE__, __LINE__, __FUNCTION__);                 \
            return ConvertResultFunc(____result);                                            \
        }                                                                                    \
        ProccessResultFunc(____result);                                                      \
    }

#define RESULT_HANDLING_GENERIC_RET(op, ConvertResultFunc, LogErrorFunc)     \
    {                                                                        \
        auto ____result = (op);                                              \
        if (!ResultHandling::IsOK(____result)) {                             \
            LogErrorFunc(____result, #op, __FILE__, __LINE__, __FUNCTION__); \
        }                                                                    \
        return ConvertResultFunc(____result);                                \
    }

#define RESULT_HANDLING_NO_CONVERT(result) status
#define RESULT_HANDLING_NO_FUNC(status)
#define RESULT_LOG_ERROR(status) status

//
//  If given operation failed, return its error status from the function that
//  calls this macros. Log error message.
//
#define RES_RIF(op)                                                                      \
    {                                                                                    \
        auto ____result = (op);                                                          \
        if (!ResultHandling::IsOK(____result)) {                                         \
            ResultHandling::LogError(____result, #op, __FILE__, __LINE__, __FUNCTION__); \
            return ____result;                                                           \
        }                                                                                \
    }

//
//  Return result of the given operation.
//  If result is failure - log it.
//
#define RES_RET(op)                                                                      \
    {                                                                                    \
        auto ____result = (op);                                                          \
        if (!ResultHandling::IsOK(____result)) {                                         \
            ResultHandling::LogError(____result, #op, __FILE__, __LINE__, __FUNCTION__); \
        }                                                                                \
        return ____result;                                                               \
    }

namespace ResultHandling {

    template <class StatusT>
    inline void LogError(const StatusT& result, const char* operationText, const char* file,
                         int line, const char* function) {
        fprintf(stderr, "ERROR %s:%d:%s {%s} %s\n", file, line, function, operationText,
                ResultHandling::ToString(result).c_str());
        fprintf(stdout, "ERROR %s:%d:%s {%s} %s\n", file, line, function, operationText,
                ResultHandling::ToString(result).c_str());
    }

}  // ResultHandling
