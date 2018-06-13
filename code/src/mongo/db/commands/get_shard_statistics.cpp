
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding

#include "mongo/db/catalog/database.h"
#include "mongo/db/catalog/database_catalog_entry.h"
#include "mongo/db/catalog/database_holder.h"
#include "mongo/db/client.h"
#include "mongo/db/commands.h"
#include "mongo/db/concurrency/d_concurrency.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/service_context.h"
#include "mongo/db/storage/storage_engine.h"
#include "mongo/db/stats/counters.h"
#include "mongo/util/version.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/util/log.h"
#include "mongo/util/processinfo.h"
#include "mongo/util/procparser.h"
#include "mongo/db/stats/counters.h"

namespace mongo {

using std::set;
using std::string;
using std::stringstream;
using std::vector;

// the name of the keys must match the names in the proc file, or will fail to parse from the proc file

//cat /proc/stat
//      user   nice  system idle iowait  irq  softirq steal guest guest_nice
//cpu  4705 356  584    3699   23    23     0       0     0          0

//user: normal processes executing in user mode
//nice: niced processes executing in user mode
//system: processes executing in kernel mode
//idle: twiddling thumbs
//iowait: waiting for I/O to complete
//irq: servicing interrupts
//softirq: servicing softirqs
//steal: involuntary wait
//guest: running a normal guest
//guest_nice: running a niced guest
static const std::vector<StringData> kCpuKeys{
    "cpu"_sd,
};

// /proc/meminfo
static const std::vector<StringData> kMemKeys{
    "MemTotal"_sd, "MemFree"_sd, "MemAvailable"_sd,
};

// /proc/self/status
// VmPeak: Peak virtual memory size.
// VmSize: Virtual memory size.
// VmRSS: Resident set size.  Note that the value here is the sum of RssAnon, RssFile, and RssShmem.
static const std::vector<StringData> kMemSelfKeys{
    "VmPeak"_sd, "VmSize"_sd, "VmRSS"_sd
};

class CmdGetShardStatistics : public Command {
public:
    CmdGetShardStatistics() : Command("getShardStatistics", true) {}

    virtual bool slaveOk() const {
        return false;
    }
    virtual bool slaveOverrideOk() const {
        return true;
    }
    virtual bool adminOnly() const {
        return true;
    }
    virtual bool supportsWriteConcern(const BSONObj& cmd) const override {
        return false;
    }
    virtual void help(stringstream& help) const {
        help << "get statistics from this shard server";
    }
    virtual Status checkAuthForOperation(OperationContext* txn,
                                         const std::string& dbname,
                                         const BSONObj& cmdObj) {
        return Status::OK();
    }
    void collectCpuInfo(BSONObjBuilder& builder) {
        ProcessInfo p;

        {   
            // cpu infomation for the whold system, by readding file /proc/stat
            BSONObjBuilder subObjBuilder(builder.subobjStart("cpu"_sd));
            subObjBuilder.append("num_cpus", p.getNumCores());

            auto status = procparser::parseProcStatFile("/proc/stat"_sd, kCpuKeys, &subObjBuilder);
            if (!status.isOK()) {
                warning() << "parse /proc/stat fail " << status.toString();
            }
            subObjBuilder.doneFast();

            // Total CPU time since boot = user+nice+system+idle+iowait+irq+softirq+steal
            // Guest and Guest_nice are already accounted in user and nice, hence they are not included in the total calculation
            // Total CPU Idle time since boot = idle + iowait
            // Total CPU usage time since boot = Total CPU time since boot - Total CPU Idle time since boot
            // Total CPU percentage = Total CPU usage time since boot/Total CPU time since boot X 100
        }

        //cat /proc/self/stat

        // utime: Amount of time that this process has been scheduled in user mode
        // stime: Amount of time that this process has been scheduled in kernel mode
        // cutime: Amount of time that this process's waited-for children have been scheduled in user mode
        // cstime: Amount of time that this process's waited-for children have been scheduled in kernel mode

        {
            // cpu infomation for current process, by readding file /proc/self/stat
            BSONObjBuilder subObjBuilder(builder.subobjStart("cpuself"_sd));
            p.getCpuInfo(subObjBuilder); 
            subObjBuilder.doneFast();
        }

        // TODO: For real time CPU usage, need to calculate the time between two intervals.
        return;
    }

    void collectMemInfo(BSONObjBuilder& builder) {
        {
            // mem infomation for whole system, by readding file /proc/meminfo
            BSONObjBuilder subObjBuilder(builder.subobjStart("memory"_sd));
            auto status = procparser::parseProcMemInfoFile("/proc/meminfo"_sd, kMemKeys, &subObjBuilder);
            if (!status.isOK()) {
                warning() << "parse /proc/meminfo fail " << status.toString();
            }
            subObjBuilder.doneFast();
        }

        {
            // mem infomation for current process, by readding file /proc/self/status
            BSONObjBuilder subObjBuilder(builder.subobjStart("memoryself"_sd));
            auto status = procparser::parseProcSelfStatusFile("/proc/self/status"_sd, kMemSelfKeys, &subObjBuilder);
            if (!status.isOK()) {
                warning() << "parse /proc/self/status fail " << status.toString();
            }
            subObjBuilder.doneFast();
        }
 
        return;
    }

    void collectNetworkInfo(BSONObjBuilder& builder) {
        BSONObj nowCounter = networkCounter.getObj();
        // TODO: calculate periodic value
        builder.append("bandwidth", nowCounter);
        return;
    }

    void collectOpsInfo(BSONObjBuilder& builder) {
        BSONObj nowCounter = globalOpCounters.getObj();
        // TODO: calculate periodic value
        builder.append("ops", nowCounter);
        return;
    }

    bool run(OperationContext* txn,
             const string& dbname,
             BSONObj& jsobj,
             int,
             string& errmsg,
             BSONObjBuilder& result) {
        if (serverGlobalParams.clusterRole == ClusterRole::ConfigServer) {
            errmsg = "getShardStatistics is not allowed on configserver";
            return false;
        }

        // "cpu"
        collectCpuInfo(result);

        // "mem"
        collectMemInfo(result);

        // "bandwidth"
        collectNetworkInfo(result);

        // "ops"
        collectOpsInfo(result);

        // "chunks"
        std::vector<std::string> dbNames;
        StorageEngine* storageEngine = getGlobalServiceContext()->getGlobalStorageEngine();
        {
            ScopedTransaction transaction(txn, MODE_IS);
            Lock::GlobalLock lk(txn->lockState(), MODE_IS, UINT_MAX);
            storageEngine->listDatabases(&dbNames);
        }

        std::vector<BSONObj> chunkInfos;
        for (const string& dbname : dbNames) {
            ScopedTransaction transaction(txn, MODE_IS);
            Lock::DBLock dbLock(txn->lockState(), dbname, MODE_IS);

            Database* db = dbHolder().get(txn, dbname);
            if (!db) {
                warning() << "can not find " << dbname;
                continue;
            }

            // go through all the collections (actually chunk)
            std::vector<NamespaceString> collectionNSs;
            db->listCollectionNSs(collectionNSs);
            for (auto collectionNS : collectionNSs) {
                Lock::CollectionLock collLock(txn->lockState(), collectionNS.ns(), MODE_IS);
                // It is possible that after we acquire the collection lock, the collection has been dropped
                Collection* collection = db->getCollection(collectionNS);
                if (!collection) {
                    continue;                    
                }
                
                std::string chunkIdstr = std::move(collection->ns().extractChunkId());

                // Ignore non-sharded collections
                // TODO: for maas,  maybe need the  non-sharded collections statistics too
                if (0 == chunkIdstr.size()) {
                    continue;
                }

                BSONObjBuilder c;
                c.append("ns", collection->ns().nsFilteredOutChunkId());
                c.append("chunkid", chunkIdstr);
                long long size = collection->dataSize(txn);
                c.append("size", size / 1024 / 1024);
                long long numRecords = collection->numRecords(txn);
                c.append("count", numRecords);
                // TODO: we need to get more info from rocksdb to calculate tps for chunk
                c.append("tps", 0);
                c.append("rocksdb", collection->getSSTFileStatistics());
                chunkInfos.push_back(c.obj());
            }
        }

        if (0 != chunkInfos.size()) {
            result.append("chunks", chunkInfos);
        }

        result.append("mongoVersion", VersionInfoInterface::instance().version());

        return true;
    }
} cmdGetShardStatistics;
}
