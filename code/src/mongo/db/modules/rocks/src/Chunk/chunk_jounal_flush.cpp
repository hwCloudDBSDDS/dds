
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "chunk_jounal_flush.h"
#include "mongo/db/client.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/util/background.h"
#include "mongo/util/log.h"
#include "mongo/util/time_support.h"
#include "mongo/util/util_extend/config_reader.h"

namespace mongo {

    ChunkJounalFlush::ChunkJounalFlush(int32_t threadNum) : _threadNum(threadNum) {
        ThreadPool::Options options;
        options.poolName = "mongorocksFlushThreadPool";
        options.threadNamePrefix = "mongorocksFlushing";
        options.maxThreads = threadNum;
        options.minThreads = threadNum > 1 ? threadNum / 2 : 1;
        options.onCreateThread = [](const std::string& threadName) {
            Client::initThread(threadName.c_str());
        };

        for (int i = 0; i < threadNum; i++) {
            std::vector<RocksDurabilityManager*> managers;
            _durabilityManagers.insert(std::make_pair(i, managers));
        }

        _running.store(true);
        _threadPool = new ThreadPool(options);
        _threadPool->startup();

        //int intervalSec = ConfigReader::getInstance()->getDecimalNumber<int>(
        //    "PublicOptions", "flush_memtable_interval");

        for (int i = 0; i < threadNum; i++) {
            _threadPool->schedule([=] {
                int pos = 0;
                //auto sTime = Date_t::now();
                while (_running.load()) {
                    try {
                        const auto startTime = Date_t::now();
                        //bool flushMemTable = Date_t::now() - sTime > Seconds(intervalSec);
                        bool finish = true;

                        do {
                            auto manager = getOneManager(i, pos, finish);
                            if(manager != nullptr) {
                                manager->waitUntilDurable(false);
                                //if (flushMemTable) {
                                //    manager->flushMemTable();
                                //    sTime = Date_t::now();
                                //}
                                flushFinish(manager);
                            }
                        } while (!finish);

                        const Milliseconds useTime = Date_t::now() - startTime;
                        long long IntervalTime = storageGlobalParams.journalCommitIntervalMs;
                        if (!IntervalTime) {
                            IntervalTime = 100;
                        }

                        if (useTime < Milliseconds(IntervalTime)) {
                            auto sleepMillis = Milliseconds(IntervalTime) - useTime;
                            stdx::this_thread::sleep_for(sleepMillis.toSteadyDuration());
                        } else {
                              index_warning() << "Flush thread use to long ,plase add flush thread num : " << useTime;
                        }
                    } catch (const UserException& e) {
                        invariant(e.getCode() == ErrorCodes::ShutdownInProgress);
                    }
                }
            });
        }
    }

    ChunkJounalFlush::~ChunkJounalFlush(void) {
        _running.store(false);
        _threadPool->shutdown();
        _threadPool->join();
        delete _threadPool;
        _threadPool = nullptr;
    }

    void ChunkJounalFlush::addDurableManager(RocksDurabilityManager* manager) {
        stdx::lock_guard<stdx::mutex> lk(_mutex);
        _durabilityManagers[currentInsertPos].push_back(manager);
        currentInsertPos++;
        if (currentInsertPos == _threadNum) {
            currentInsertPos = 0;
        }
    }

    void ChunkJounalFlush::delDurableManager(RocksDurabilityManager* manager) {
        {
            stdx::lock_guard<stdx::mutex> lk(_mutex);
            bool finish = false;
            for (int i = 0; (i < _threadNum) & !finish; i++) {
                for (auto it = _durabilityManagers[i].begin(); it != _durabilityManagers[i].end();
                     ++it) {
                    if (*it == manager) {
                        _durabilityManagers[i].erase(it);
                        finish = true;
                        break;
                    }
                }
            }
        }

        while (true) {
            {
                stdx::lock_guard<stdx::mutex> lk(_mutex);
                if (_flushing.find(manager) == _flushing.end()) {
                    return;
                }
            }

            sleepmillis(10);
        }
    }

    RocksDurabilityManager* ChunkJounalFlush::getOneManager(int current, int& pos, bool& finish) {
        stdx::lock_guard<stdx::mutex> lk(_mutex);
        if (_durabilityManagers[current].empty()) {
            finish = true;
            return nullptr;
        }

        if (pos >= (int)(_durabilityManagers[current].size())) {
            finish = true;
            pos = 0;
        } else {
            finish = false;
        }

        auto manger = _durabilityManagers[current][pos];
        _flushing.insert(manger);
        pos++;
        return manger;
    }

    void ChunkJounalFlush::flushFinish(RocksDurabilityManager* manager) {
        stdx::lock_guard<stdx::mutex> lk(_mutex);
        _flushing.erase(manager);
    }
}
