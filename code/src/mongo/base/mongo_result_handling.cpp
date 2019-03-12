
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo_result_handling.h"
#include "mongo/util/log.h"

namespace ResultHandling {

void LogError(const mongo::Status& result,
              const char* operationText,
              const char* file,
              int line,
              const char* function) {
    mongo::log() << "[RES_ERROR][MongoDB] " << file << ":" << line << ":" << function << " {"
                 << operationText << "} " << ResultHandling::ToString(result);
}

}  // ResultHandling
