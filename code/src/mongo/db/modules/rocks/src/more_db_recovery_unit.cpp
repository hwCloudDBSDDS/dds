
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include <rocksdb/comparator.h>
#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <rocksdb/perf_context.h>
#include <rocksdb/slice.h>
#include <rocksdb/utilities/write_batch_with_index.h>
#include <rocksdb/write_batch.h>

#include "Chunk/ChunkRocksDBInstance.h"
#include "mongo/util/log.h"
#include "more_db_recovery_unit.h"
#include "rocks_snapshot_manager.h"
#include "rocks_snapshot_manager.h"
#include "rocks_transaction.h"
#include "rocks_util.h"

namespace mongo {

    MoreDbRocksRecoveryUnit::~MoreDbRocksRecoveryUnit() {
        _abort();
        for (auto it = _snapshots.begin(); it != _snapshots.end(); ++it) {
            if (it->second) {
                it->first->GetDB()->ReleaseSnapshot(it->second);
            }
        }

        for (auto it = _writeBatchs.begin(); it != _writeBatchs.end(); ++it) {
            it->second->Clear();
        }

        _writeBatchs.clear();
        _snapshots.clear();
        _moreTransaction.clear();
    }

    void MoreDbRocksRecoveryUnit::commitUnitOfWork() {
        for (auto it = _writeBatchs.begin(); it != _writeBatchs.end(); ++it) {
            if (it->second->GetWriteBatch()->Count() > 0) {
                _commit(_txn);
                break;
            }
        }

        _releaseSnapshot();

        try {
            for (Changes::const_iterator it = _changes.begin(), end = _changes.end(); it != end;
                 ++it) {
                (*it)->commit();
            }
            _changes.clear();
        } catch (...) {
            std::terminate();
        }
    }

    bool MoreDbRocksRecoveryUnit::waitUntilDurable() {
        for (auto it = _writeBatchs.begin(); it != _writeBatchs.end(); ++it) {
            if (it->first == _db) {
                continue;
            } else {
                it->first->GetDurabilityManager()->waitUntilDurable(false);
            }
        }

        _db->GetDurabilityManager()->waitUntilDurable(false);
        return true;
    }

    void MoreDbRocksRecoveryUnit::abandonSnapshot() {
        for (auto it = _writeBatchs.begin(); it != _writeBatchs.end(); ++it) {
            it->second->Clear();
        }
        _releaseSnapshot();
        _areWriteUnitOfWorksBanned = false;
        _moreDBDeltaCounters.clear();
    }

    bool MoreDbRocksRecoveryUnit::hasSnapshot() {
        for (auto it = _snapshots.begin(); it != _snapshots.end(); ++it) {
            if (it->second) {
                return true;
            }
        }

        return false;
    }

    rocksdb::WriteBatchWithIndex* MoreDbRocksRecoveryUnit::writeBatch() {
        auto it = _writeBatchs.find(_db);
        if (it == _writeBatchs.end()) {
            _writeBatchs[_db] = std::unique_ptr<rocksdb::WriteBatchWithIndex>(
                new rocksdb::WriteBatchWithIndex(rocksdb::BytewiseComparator(), 0, true));
            it = _writeBatchs.find(_db);
        }

        return it->second.get();
    }

    const rocksdb::Snapshot* MoreDbRocksRecoveryUnit::snapshot() {
        if (_readFromMajorityCommittedSnapshot) {
            index_err() << "snapshot()-> faild not support readConcern:majority!";
            if (_snapshotHolder.get() == nullptr) {
                _snapshotHolder = _snapshotManager->getCommittedSnapshot();
            }

            invariant(false);
            return nullptr;
        }

        auto it = _snapshots.find(_db);
        if (_snapshots.end() == it) {
            auto ts = transaction();
            if (ts) {
                ts->recordSnapshotId();
            }

            _snapshots[_db] = _db->GetDB()->GetSnapshot();
            it = _snapshots.find(_db);
        }
        return it->second;
    }

    rocksdb::Status MoreDbRocksRecoveryUnit::Get(const rocksdb::Slice& key, std::string* value,
                                                 rocksdb::ColumnFamilyHandle* cf) {
        cf = ResolveColumnFamily(cf);
        auto wb = writeBatch();
        if (wb) {
            if (wb->GetWriteBatch()->Count() > 0) {
                std::unique_ptr<rocksdb::WBWIIterator> wb_iterator(writeBatch()->NewIterator(cf));
                wb_iterator->Seek(key);
                if (wb_iterator->Valid() && wb_iterator->Entry().key == key) {
                    const auto& entry = wb_iterator->Entry();
                    if (entry.type == rocksdb::WriteType::kDeleteRecord) {
                        return rocksdb::Status::NotFound();
                    }
                    *value = std::string(entry.value.data(), entry.value.size());
                    return rocksdb::Status::OK();
                }
            }
        }

        rocksdb::ReadOptions options;
        options.snapshot = snapshot();
        return getDB()->Get(options, cf, key, value);
    }

    void MoreDbRocksRecoveryUnit::_releaseSnapshot() {
        for (auto it = _snapshots.begin(); it != _snapshots.end(); ++it) {
            if (it->second) {
                it->first->GetDB()->ReleaseSnapshot(it->second);
                _snapshots[it->first] = nullptr;
                if (_moreTransaction.find(it->first) != _moreTransaction.end()) {
                    _moreTransaction[it->first]->abort();
                    _moreTransaction.erase(it->first);
                } else {
                    // nothing
                }
            }
        }

        _snapshots.clear();
        _moreTransaction.clear();
        _snapshotHolder.reset();
        _myTransactionCount++;
    }

    void MoreDbRocksRecoveryUnit::_commit(OperationContext* txn) {
        for (auto it = _writeBatchs.begin(); it != _writeBatchs.end(); ++it) {
            rocksdb::WriteBatch* wb = it->second->GetWriteBatch();
            if (wb->Count() != 0) {
                bool isSplitOnGoing = it->first->GetTransactionEngine()->getSplitIsOnGoing();
                throwExceptionIfWriteBatchBelongsToChildSide(txn, it->second.get(), it->first,
                                                             isSplitOnGoing);
                addCountOfActive(isSplitOnGoing, it->first);

                rocksdb::Status status;
                try {
                    for (auto pair : _moreDBDeltaCounters) {
                        for (auto delta : pair.second) {
                            auto& counter = delta.second;

                            counter._value->fetch_add(counter._delta,
                                                      std::memory_order::memory_order_relaxed);
                            long long newValue =
                                counter._value->load(std::memory_order::memory_order_relaxed);

                            pair.first->GetCounterManager()->updateCounter(delta.first, newValue,
                                                                           nullptr);
                        }
                    }
                    _moreDBDeltaCounters.clear();

                    // Order of operations here is important. It needs to be synchronized with
                    rocksdb::WriteOptions writeOptions;
                    writeOptions.disableWAL = !it->first->IsDurable();
                    status = it->first->GetDB()->Write(writeOptions, wb);
                } catch (const std::exception& e) {
                    decCountOfActive(isSplitOnGoing, it->first);
                    throw e;
                }

                decCountOfActive(isSplitOnGoing, it->first);

                // rocksdb return err but not timeout,return error
                if (status.IsTimedOut()) {
                    invariantRocksOKWithNoCore(status);
                } else if (!status.ok()) {
                    index_err() << "commite return err,errcode:" << status.ToString();
                    uassertStatusOK(rocksToMongoStatus(status));
                }

                if (_moreTransaction.find(it->first) != _moreTransaction.end()) {
                    _moreTransaction[it->first]->commit();
                }
            }

            it->second->Clear();
        }

        _moreTransaction.clear();
    }

    void MoreDbRocksRecoveryUnit::_abort() {
        try {
            for (Changes::const_reverse_iterator it = _changes.rbegin(), end = _changes.rend();
                 it != end; ++it) {
                Change* change = *it;
                change->rollback();
            }
            _changes.clear();
        } catch (...) {
            std::terminate();
        }

        _moreDBDeltaCounters.clear();
        for (auto it = _writeBatchs.begin(); it != _writeBatchs.end(); ++it) {
            it->second->Clear();
        }

        _releaseSnapshot();
    }

    void MoreDbRocksRecoveryUnit::incrementCounter(const rocksdb::Slice& counterKey,
                                                   std::atomic<long long>* counter,
                                                   long long delta) {
        if (0 == delta) {
            return;
        }

        auto it = _moreDBDeltaCounters.find(_db);
        if (it == _moreDBDeltaCounters.end()) {
            CounterMap deltaCounters;
            _moreDBDeltaCounters[_db] = deltaCounters;
            it = _moreDBDeltaCounters.find(_db);
        }

        auto pair = it->second.find(counterKey.ToString());
        if (pair == it->second.end()) {
            it->second[counterKey.ToString()] = mongo::RocksRecoveryUnit::Counter(counter, delta);
        } else {
            pair->second._delta += delta;
        }
    }

    long long MoreDbRocksRecoveryUnit::getDeltaCounter(const rocksdb::Slice& counterKey) {
        auto it = _moreDBDeltaCounters.find(_db);
        if (it == _moreDBDeltaCounters.end()) {
            return 0;
        }

        auto counter = it->second.find(counterKey.ToString());
        if (counter == it->second.end()) {
            return 0;
        } else {
            return counter->second._delta;
        }
    }

    RocksTransaction* MoreDbRocksRecoveryUnit::transaction() {
        auto it = _moreTransaction.find(_db);
        if (it == _moreTransaction.end()) {
            _moreTransaction[_db] = std::unique_ptr<RocksTransaction>(
                new RocksTransaction(_db->GetTransactionEngine()));
            return _moreTransaction[_db].get();
        } else {
            return it->second.get();
        }
    }

}  //  namespace mongo