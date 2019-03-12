#pragma once

#include <mongo/stdx/memory.h>
#include <atomic>
#include "mongo/stdx/mutex.h"
#include "mongo/stdx/thread.h"
#include "mongo/db/stats/counters.h"
#include "mongo/db/operation_context.h"
namespace mongo {

    class OperationContext;

    //
    // The StatisticsCollector is a background task that tries to collect statics on a
    // single shard server.
    //
    // The StatisticsCollector does act continuously but in "rounds". 
    // Each shard server has an independent StatisticsCollector thread
    //
    class StatisticsCollector {

    public:
         struct CpuOccupancy {
            std::string name;
            int64_t user;
            int64_t nice;
            int64_t system;
            int64_t idle;
            int64_t iowait;
            int64_t irq;
            int64_t softirq;
        } ;
    public:
        StatisticsCollector();

        StatisticsCollector(StatisticsCollector const &) = delete;

        void operator=(StatisticsCollector const &) = delete;

        void start();

        void stop();


    private:
        //
        // Possible runtime states of the StatisticsCollector. The comments indicate the allowed next state.
        //
        enum State {
            kStopped,  // kStopping
            kRunning,  // kRunning
            kPause,
        };


        void waitForStorageEngineToBeCreated() ;

        //
        // The main StatisticsCollector loop, which runs in a separate thread.
        //
        void _mainThread();

        //
        // Checks whether the StatisticsCollector main thread has been requested to stop.
        //
        bool _stopRequested();

        //
        // Signals the beginning and end of a StatisticsCollector round.
        //
        void _beginRound(OperationContext *txn);

        void _endRound(OperationContext *txn, Seconds waitTimeout);

        //N
        // Blocks the caller for the specified timeout or until the gc client condition variable is
        // signaled, whichever comes first.
        //
        void _sleepFor(OperationContext *txn, Seconds waitTimeout);

        void collectChunkTps(int roundIntervalSeconds);


        void resetThreadOperationContext(ServiceContext::UniqueOperationContext& txn,
                                                              stdx::mutex& gcMutex,
                                                              OperationContext** threadOperationContext) ;

        StatusWith<CpuOccupancy> parseCpu(const BSONObj& source);

        Status getCpuProcTime(BSONObjBuilder &builder);

        int getCpuRate();

        int calCpuOccupy (CpuOccupancy &o, CpuOccupancy &n);
        //
        // Indicates the current state of the gc client
        //
        std::atomic<State> _state{kStopped};

        //
        // Protects the state below
        //
        stdx::mutex _mutex;

        //
        // The main StatisticsCollector thread
        //
        stdx::thread _thread;

        //
        // The operation context of the main StatisticsCollector thread. This value may only be available in
        // the
        // kRunning state and is used to force interrupt of any blocking calls made by the gc client
        // thread.
        //
        OperationContext *_threadOperationContext{nullptr};


        //
        // Condition variable, which is signalled every time the above runtime state of the StatisticsCollector
        // changes (in particular, state/StatisticsCollector round and number of StatisticsCollector rounds).
        //
        stdx::condition_variable _condVar;

    };

}

