#pragma once

#include "rocks_result_handling.h"
#include <mongo/base/mongo_result_handling.h>
#include "result_handling.h"
#include "rocks_util.h"


namespace ResultHandling
{

inline mongo::Status MongoConvertStatus(const rocksdb::Status& status)
{
    return mongo::rocksToMongoStatus(status);
}

class MongoRocksLogger : public rocksdb::Logger
{
public:
    MongoRocksLogger() : rocksdb::Logger(rocksdb::InfoLogLevel::ERROR_LEVEL) { }

    // Write an entry to the log file with the specified format.
    virtual void Logv(const char* format, va_list ap) override; 
    using rocksdb::Logger::Logv;
};

class MongoRocksLoggerForChunk : public rocksdb::Logger
{
    std::string chunkTag;
public:
    MongoRocksLoggerForChunk(std::string chunkTag) :
        rocksdb::Logger(_level), chunkTag(std::move(chunkTag)) { }

    // Write an entry to the log file with the specified format.
    virtual void Logv(const char* format, va_list ap) override; 
    using rocksdb::Logger::Logv;

    void SetTag(std::string tag);

    static void setLogLevel(rocksdb::InfoLogLevel level){
        _level = level;
    }

    static rocksdb::InfoLogLevel _level;
};

}  // ResultHandling

//  Some buffer formatting functions. TODO: find a better place for them.
namespace mongo
{

std::string ToString(const char* buf, size_t size);

inline std::string ToString(const rocksdb::Slice& slice)
{
    return ToString(slice.data(), slice.size());
}

}

