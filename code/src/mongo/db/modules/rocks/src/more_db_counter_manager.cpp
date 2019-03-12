#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage
#include "more_db_counter_manager.h"
#include <rocksdb/db.h>
#include <rocksdb/utilities/write_batch_with_index.h>
#include <rocksdb/write_batch.h>
#include "mongo/util/log.h"
#include "rocks_util.h"

namespace mongo {

    void MoreDBCounterManager::updateCounter(const std::string& counterKey, long long count,
                                             rocksdb::WriteBatch* writeBatch, bool persist) {
        updateCounter(counterKey, count, persist);
    }

    // Direct persistence
    void MoreDBCounterManager::updateCounter(const std::string& counterKey, long long count,
                                             bool persist) {
        int64_t storage = 0;
        rocksdb::WriteBatch writeBatch;
        if (_crashSafe) {
            writeBatch.Put(_columnFamily, counterKey, _encodeCounter(count, &storage));
        } else {
            stdx::lock_guard<stdx::mutex> lk(_lock);
            _counters[counterKey] = count;
            ++_syncCounter;
            if (!_syncing && (_syncCounter >= kSyncEvery || persist)) {
                // let's sync this now. piggyback on writeBatch
                for (const auto& counter : _counters) {
                    writeBatch.Put(_columnFamily, counter.first,
                                   _encodeCounter(counter.second, &storage));
                }
                _counters.clear();
                _syncCounter = 0;
            }
        }

        if (writeBatch.Count() != 0) {
            auto status = _db->Write(rocksdb::WriteOptions(), &writeBatch);
            invariantRocksOK(status);
        }
    }
}