
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo_rocks_result_handling.h"
#include "rocks_util.h"

#include "mongo/util/log.h"

#include <stdarg.h>
#include <stdio.h>

#include <string.h>
#include <iomanip>
#include <sstream>
#include "mongo/util/util_extend/config_reader.h"

namespace ResultHandling {

    rocksdb::InfoLogLevel getLogLevel() {
        std::string level =
            ConfigReader::getInstance()->getString("PublicOptions", "rocksdb_log_level");
        if ("DEBUG_LEVEL" == level) {
            return rocksdb::InfoLogLevel::DEBUG_LEVEL;
        } else if ("INFO_LEVEL" == level) {
            return rocksdb::InfoLogLevel::INFO_LEVEL;
        } else if ("WARN_LEVEL" == level) {
            return rocksdb::InfoLogLevel::WARN_LEVEL;
        } else if ("ERROR_LEVEL" == level) {
            return rocksdb::InfoLogLevel::ERROR_LEVEL;
        } else if ("FATAL_LEVEL" == level) {
            return rocksdb::InfoLogLevel::FATAL_LEVEL;
        } else if ("HEADER_LEVEL" == level) {
            return rocksdb::InfoLogLevel::HEADER_LEVEL;
        } else {
            return rocksdb::InfoLogLevel::WARN_LEVEL;
        }
    }

    rocksdb::InfoLogLevel MongoRocksLoggerForChunk::_level =
        rocksdb::InfoLogLevel::NUM_INFO_LOG_LEVELS;

    void MongoRocksLogger::Logv(const char* format, va_list ap) {
        char buffer[8192];
        int len = snprintf(buffer, sizeof(buffer) - 1, "[RES_ERROR][RocksDB]");
        if (0 > len) {
            mongo::log() << "MongoRocksLogger::Logv return NEGATIVE value.";
            return;
        }
        vsnprintf(buffer + len, sizeof(buffer) - len - 1, format, ap);
        mongo::log() << buffer;
    }

    void MongoRocksLoggerForChunk::Logv(const char* format, va_list ap) {
        char buffer[8192];
        int len = snprintf(buffer, sizeof(buffer) - 1, "[RocksDB][%s]",
                             chunkTag.c_str());
        if (0 > len) {
            mongo::log() << "MongoRocksLoggerForChunk::Logv return NEGATIVE value.";
            return;
        }
        vsnprintf(buffer + len, sizeof(buffer) - len - 1, format, ap);
        mongo::log() << buffer;
    }

    void MongoRocksLoggerForChunk::SetTag(std::string tag) { chunkTag = std::move(tag); }

    rocksdb::Logger* RocksDBLogging::logger = nullptr;

    void LogError(const rocksdb::Status& result, const char* operationText, const char* file,
                  int line, const char* function) {
        rocksdb::Logger* logger = RocksDBLogging::Logger();

        if (logger == nullptr) {
            fprintf(stderr, "ERROR %s:%d:%s {%s} %s\n", file, line, function, operationText,
                    ResultHandling::ToString(result).c_str());
            fprintf(stdout, "ERROR %s:%d:%s {%s} %s\n", file, line, function, operationText,
                    ResultHandling::ToString(result).c_str());
            return;
        }

        rocksdb::Log(rocksdb::InfoLogLevel::ERROR_LEVEL, logger, "ERROR %s:%d:%s {%s} %s\n", file,
                     line, function, operationText, ResultHandling::ToString(result).c_str());
    }

}  // ResultHandling

namespace mongo {

    std::string ToString(const char* buf, size_t size) {
        std::string str;
        str.reserve(size);
        std::stringstream ss(str);
        bool unprintable = false;
        for (size_t i = 0; i < size; i++) {
            char c = buf[i];
            if (isprint(c)) {
                if (unprintable) {
                    ss << "}";
                    unprintable = false;
                }

                if (c == '{') {
                    ss << "{{";
                    continue;
                }

                if (c == '}') {
                    ss << "}}";
                    continue;
                }

                ss << c;
                continue;
            }

            if (!unprintable) {
                ss << "{";
                unprintable = true;
            } else
                ss << "|";

            ss << std::hex << std::setfill('0') << std::setw(2) << (int)c;
        }

        if (unprintable) ss << "}";

        return ss.str();
    }
}
