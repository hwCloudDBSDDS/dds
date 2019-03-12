#pragma once

#include <map>
#include "Chunk/ChunkRocksDBInstance.h"
#include "rocks_recovery_unit.h"

namespace mongo {
    class RocksSnapshotManager;
}

namespace mongo {

    class MoreDbRocksRecoveryUnit : public RocksRecoveryUnit {
    public:
        MoreDbRocksRecoveryUnit(RocksSnapshotManager* snapshotManager,
                                ChunkRocksDBInstance* dbInstance)
            : RocksRecoveryUnit(snapshotManager, dbInstance) {}

        ~MoreDbRocksRecoveryUnit();

        virtual void commitUnitOfWork() override;

        virtual bool waitUntilDurable() override;

        virtual void abandonSnapshot() override;

        virtual bool hasSnapshot() override;

        virtual rocksdb::WriteBatchWithIndex* writeBatch() override;

        virtual const rocksdb::Snapshot* snapshot() override;

        virtual rocksdb::Status Get(const rocksdb::Slice& key, std::string* value,
                                    rocksdb::ColumnFamilyHandle* cf = nullptr);

        virtual void incrementCounter(const rocksdb::Slice& counterKey,
                                      std::atomic<long long>* counter, long long delta) override;

        virtual long long getDeltaCounter(const rocksdb::Slice& counterKey) override;

        virtual RocksRecoveryUnit* newRocksRecoveryUnit() {
            return new MoreDbRocksRecoveryUnit(GetSnapshotManager(), _db);
        }

        virtual RocksTransaction* transaction() override;

        virtual void _releaseSnapshot() override;

        virtual void _commit(OperationContext* txn) override;

        virtual void _abort() override;

    private:
        std::map<ChunkRocksDBInstance*, const rocksdb::Snapshot*> _snapshots;

        std::map<ChunkRocksDBInstance*, std::unique_ptr<rocksdb::WriteBatchWithIndex> >
            _writeBatchs;

        std::map<ChunkRocksDBInstance*, CounterMap> _moreDBDeltaCounters;

        std::map<ChunkRocksDBInstance*, std::unique_ptr<RocksTransaction> > _moreTransaction;
    };
}