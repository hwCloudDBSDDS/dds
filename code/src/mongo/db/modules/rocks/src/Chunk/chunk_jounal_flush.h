#include <list>
#include <map>
#include <set>
#include "../rocks_durability_manager.h"
#include "mongo/util/concurrency/thread_pool.h"

#pragma once
namespace mongo {

    class ShardSvrJournalFlusher;
    class RocksDurabilityManager;
    class ChunkJounalFlush;

    class ChunkJounalFlush {
    public:
        ChunkJounalFlush(int32_t threadNum);
        ~ChunkJounalFlush();

        void addDurableManager(RocksDurabilityManager* durabilityManager);
        void delDurableManager(RocksDurabilityManager* durabilityManager);

    private:
        RocksDurabilityManager* getOneManager(int current, int& pos, bool& finish);
        void flushFinish(RocksDurabilityManager*);

        stdx::mutex _mutex;

        typedef std::vector<RocksDurabilityManager*>::iterator ManagerIter;
        std::map<int, std::vector<RocksDurabilityManager*> > _durabilityManagers;
        std::set<RocksDurabilityManager*> _flushing;
        int currentInsertPos = 0;

        ThreadPool* _threadPool;
        std::atomic<bool> _running{false};
        int _threadNum;
    };
}
