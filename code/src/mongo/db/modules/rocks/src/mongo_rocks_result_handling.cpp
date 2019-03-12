
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo_rocks_result_handling.h"
#include "rocks_util.h"

#include "mongo/util/log.h"

#include <stdio.h>
#include <stdarg.h>

#include <string.h>
#include <sstream>
#include <iomanip>

namespace ResultHandling
{

rocksdb::InfoLogLevel MongoRocksLoggerForChunk::_level = rocksdb::InfoLogLevel::WARN_LEVEL;

void MongoRocksLogger::Logv(const char* format, va_list ap)
{
    char buffer[8192];
    int len = snprintf(buffer, sizeof(buffer), "[RES_ERROR][RocksDB]");
    vsnprintf(buffer+len, sizeof(buffer)-len, format, ap);
    mongo::log() << buffer;
}

void MongoRocksLoggerForChunk::Logv(const char* format, va_list ap)
{
    char buffer[8192];
    int len = snprintf(buffer, sizeof(buffer), "[RocksDB][%s]", chunkTag.c_str());
    vsnprintf(buffer+len, sizeof(buffer)-len, format, ap);
    mongo::log() << buffer;
}

void MongoRocksLoggerForChunk::SetTag(std::string tag)
{
    chunkTag = std::move(tag);
}

rocksdb::Logger* RocksDBLogging::logger = nullptr;

void LogError(
              const rocksdb::Status& result,
              const char* operationText,
              const char* file,
              int line,
              const char* function)
{
    rocksdb::Logger* logger = RocksDBLogging::Logger();

    if(logger == nullptr)
    {
        fprintf(stderr, "ERROR %s:%d:%s {%s} %s\n",
            file, line, function, operationText, ResultHandling::ToString(result).c_str());
        fprintf(stdout, "ERROR %s:%d:%s {%s} %s\n",
            file, line, function, operationText, ResultHandling::ToString(result).c_str());
        return;
    }

    rocksdb::Log(
        rocksdb::InfoLogLevel::ERROR_LEVEL, logger,
        "ERROR %s:%d:%s {%s} %s\n",
        file, line, function, operationText, ResultHandling::ToString(result).c_str());   
}

}  // ResultHandling


namespace mongo
{

std::string ToString(const char* buf, size_t size)
{
    std::string str;
    str.reserve(size);
    std::stringstream ss(str);
    bool unprintable = false;
    for(size_t i = 0; i < size; i++)
    {
        char c = buf[i];
        if(isprint(c))
        {
            if(unprintable)
            {
                ss << "}";
                unprintable = false;
            }

            if(c == '{')
            {
                ss << "{{";
                continue;
            }
            
            if(c == '}')
            {
                ss << "}}";
                continue;
            }
                
            ss << c;
            continue;
        }

        if(!unprintable)
        {
            ss << "{";
            unprintable = true;
        }
        else
            ss << "|";
    
        ss << std::hex << std::setfill('0') << std::setw(2) << (int)c;
    }

    if(unprintable)
        ss << "}";

    return ss.str();
}

}
