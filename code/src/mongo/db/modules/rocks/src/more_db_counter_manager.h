
#pragma once

#include "rocks_counter_manager.h"

namespace mongo {

    class MoreDBCounterManager : public RocksCounterManager {
    public:
        MoreDBCounterManager(rocksdb::DB* db, bool crashSafe,
                             rocksdb::ColumnFamilyHandle* columnFamily)
            : RocksCounterManager(db, crashSafe, columnFamily) {}

        // if persist is true, the count will immediately write to rocksdb
        virtual void updateCounter(const std::string& counterKey, long long count,
                                   rocksdb::WriteBatch* writeBatch, bool persist = false) override;

    private:
        // Direct persistence
        void updateCounter(const std::string& counterKey, long long count, bool persist = false);
    };
}