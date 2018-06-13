// index_create.cpp

/**
*    Copyright (C) 2008-2014 MongoDB Inc.
*
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU Affero General Public License for more details.
*
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*
*    As a special exception, the copyright holders give permission to link the
*    code of portions of this program with the OpenSSL library under certain
*    conditions as described in each individual source file and distribute
*    linked combinations including the program with the OpenSSL library. You
*    must comply with the GNU Affero General Public License in all respects for
*    all of the code used other than as permitted herein. If you modify file(s)
*    with this exception, you may extend this exception to your version of the
*    file(s), but you are not obligated to do so. If you do not wish to do so,
*    delete this exception statement from your version. If you delete this
*    exception statement from all source files in the program, then also delete
*    it in the license file.
*/

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kIndex

#include "mongo/platform/basic.h"
#include <vector>
#include "mongo/db/catalog/index_create.h"

#include "mongo/base/error_codes.h"
#include "mongo/client/dbclientinterface.h"
#include "mongo/db/audit.h"
#include "mongo/db/background.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/client.h"
#include "mongo/db/clientcursor.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/curop.h"
#include "mongo/db/exec/working_set_common.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/query/internal_plans.h"
#include "mongo/db/repl/replication_coordinator_global.h"
#include "mongo/db/server_parameters.h"
#include "mongo/stdx/mutex.h"
#include "mongo/util/fail_point.h"
#include "mongo/util/fail_point_service.h"
#include "mongo/util/log.h"
#include "mongo/util/processinfo.h"
#include "mongo/util/progress_meter.h"
#include "mongo/util/quick_exit.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/client/shard.h"
#include "mongo/s/catalog/sharding_catalog_client.h"
#include "mongo/s/grid.h"
#include "mongo/s/catalog/type_collection.h"
#include "mongo/db/catalog/create_collection.h"
#include "mongo/s/client/shard_remote.h"
#include "mongo/client/remote_command_targeter.h"
#include "mongo/util/net/hostandport.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/modules/rocks/src/GlobalConfig.h"

namespace mongo {

using std::unique_ptr;
using std::string;
using std::endl;
using std::vector;
const ReadPreferenceSetting kPrimaryOnlyReadPreference{ReadPreference::PrimaryOnly};
Status passCreateIndexCMDtoEveryChunk(OperationContext* txn,
                 const NamespaceString& ns,
                 BSONArray& toCreateCmdBuilder,
                 std::string &s,
                 const std::string& dbname,
                 BSONObjBuilder& result){


    vector<ChunkType> chunks;
    uassertStatusOK(grid.catalogClient(txn)->getChunks(txn,
                BSON(ChunkType::ns(ns.ns())),
                BSONObj(),
                0,
                &chunks,
                nullptr,
                repl::ReadConcernLevel::kMajorityReadConcern));

    for(ChunkType chunk: chunks){
        log()<<chunk.toString();
        BSONObjBuilder builder;
        builder.append("createIndexes",s);
        builder.append("indexes",toCreateCmdBuilder);
        const auto shardStatus = grid.shardRegistry()->getShard(txn, chunk.getShard());
        if( !shardStatus.isOK()){
            log()<<"[passCreateIndexCMDtoEveryChunk] get shard fail";
            return shardStatus.getStatus();
        }
        builder.append("chunkId",chunk.getName());
        BSONObj cmdObj = builder.obj(); 
        const auto createindexShard = shardStatus.getValue();
        int retryNum = 0;
        while(retryNum < 1000){
            auto cmdResponseStatus = uassertStatusOK(createindexShard->runCommand(txn,kPrimaryOnlyReadPreference,dbname,cmdObj,Shard::RetryPolicy::kIdempotent));
            if (!cmdResponseStatus.commandStatus.isOK()){
                if( cmdResponseStatus.commandStatus.code() == ErrorCodes::ChunkNotAssigned ){
                   log()<<"[passCreateIndexCMDtoEveryChunk]run cmd fail, cmdObj:" << cmdObj;
                   sleepsecs(1);
                   retryNum++;
                   continue;
                }
                return cmdResponseStatus.commandStatus;
            }else{
                auto targeter = createindexShard->getTargeter(); 
                auto res = targeter->findHost(txn,ReadPreferenceSetting{ReadPreference::PrimaryOnly});
                result.append(StringData(res.getValue().toString()),cmdResponseStatus.response);
                break;// ok and other error break.
            }
        } 
    }
    return Status::OK();
}

Status createNewCollection(OperationContext* txn,const NamespaceString& ns){
    BSONObj idIndexSpec;
    BSONObjBuilder cmd;
    cmd.append("create",ns.coll());
    return createCollectionMetadata(txn, ns, cmd.obj(), idIndexSpec);
}

Status createIndexMetadata(OperationContext* txn,
        const NamespaceString& ns,
        std::vector<BSONObj>& indexSpecs,
        BSONObj& cmdObj,
        const std::string& dbname,
        BSONObjBuilder& result){

    BSONObj obj = indexSpecs[0];
    log() << "createIndexMetadata start,ns:"<< ns << ", indexSpecs[0]:" << obj;
    // check if the collection already exists , load it. If not, create a new collection first
    BSONObjBuilder subobj(result.subobjStart("raw"));
	CollectionType coll;
    auto collStatus = grid.catalogClient(txn)->getCollection(txn, ns.ns());
    //log() << ns.ns() << (collStatus.getValue().value.toString());
    if (collStatus.isOK()){
        coll = collStatus.getValue().value;
        if(coll.getDropped()){
            Status status = createNewCollection(txn, ns);
            if(!status.isOK())
                return status;
            auto cs = grid.catalogClient(txn)->getCollection(txn, ns.ns());
            coll = cs.getValue().value;
        }
    }
    else{
        // create collection
        Status status = createNewCollection(txn, ns);
        if(!status.isOK())
            return status;
        auto cs = grid.catalogClient(txn)->getCollection(txn, ns.ns());
        coll = cs.getValue().value;
    }
    // get current index specs
    BSONArrayBuilder indexArrayBuilder;
    BSONArrayBuilder toCreateCmdBuilder;
    BSONObjIterator indexspecs(coll.getIndex());
    long long ll_max_prefix = coll.getPrefix();
    while (indexspecs.more()) {
        BSONObj existingIndex = indexspecs.next().Obj();
        //indexArrayBuilder.append (existingIndex);
        bool isAppend = false;
        //remove duplicate index
        std::vector<BSONObj>::iterator it = indexSpecs.begin();
        for(;it !=indexSpecs.end();it++)
        {

            bool equal = IndexDescriptor::isIdIndexPatternEqual((*it)["key"].Obj(),existingIndex["key"].Obj());
            if((*it)["key"].toString() == existingIndex["key"].toString() || equal ){
                log() << ns.ns() << "createIndexMetadata duplicate index:" << existingIndex["name"].toString();
                BSONObj option = coll.getOptions();
                auto collation_exist = existingIndex["collation"];
                auto collation_it = (*it)["collation"];
                auto collation_option = option["collation"];
                bool simple = false;
                if( !collation_it.eoo()){
                    if(((*it)["collation"].Obj())["locale"].toString() == BSON("locale"<<"simple")["locale"].toString()){
                        simple = true;
                    }
                }
                if( !collation_it.eoo() && collation_exist.eoo() && !simple ){
                    if( collation_option.eoo()){ 
                        break;
                    }
                }else if( !collation_it.eoo() && !collation_exist.eoo() ){
                    break;      
                    /*if((collation_it.Obj())["locale"].toString() != (collation_exist.Obj())["locale"].toString()){
                        break;
                    }*/
                }
                if(!IndexDescriptor::isIdIndexPattern(existingIndex["key"].Obj())){
                    if((*it)["name"].toString() != existingIndex["name"].toString()){
                        break;
                    } 
                }       
                indexSpecs.erase(it);
                if(IndexDescriptor::isIdIndexPattern(existingIndex["key"].Obj())){
                    if( !option.isEmpty() ){
                        if( !option["autoIndexId"].eoo() && !option["autoIndexId"].boolean()){
                            option = option.removeField("autoIndexId");
                            coll.setOptions(option);
                        }
                        if( !option["collation"].eoo() && !(*it)["collation"].eoo() ){
                            if((option["collation"].Obj())["locale"].toString()!=((*it)["collation"].Obj())["locale"].toString()){
                                return {ErrorCodes::BadValue,
                                       str::stream() << "The _id index must have the same collation as the collection. Index collation: " << (*it)["collation"].toString() << ", collection collation: "<<option["collation"].toString()};
                            }
                        }else if( !option["collation"].eoo() || !(*it)["collation"].eoo()){
                            if(!(*it)["collation"].eoo() && ((*it)["collation"].Obj())["locale"].toString() != BSON("locale"<<"simple")["locale"].toString()){
                                return {ErrorCodes::BadValue, str::stream() << "The _id index must have the same collation as the collection. Index collation: " << ((*it)["collation"].eoo()? BSON("locale"<<"simple"):(*it)["collation"].Obj()) << ", collection collation: "<<(option["collation"].eoo() ? BSON("locale"<<"simple"):option["collation"].Obj())};
                            }
                       }
                       BSONObjBuilder in;
                       in.appendElements(existingIndex);
                       bool flag = false;
                       if(!option["collation"].eoo()){
                           flag = true;
                           CollationSpec spec;
                           spec.localeID = option["collation"].Obj().getField("locale").str();
                           spec.version = "57.1";
                           in.append("collation",spec.toBSON());
                       }
                       if( flag && (!(*it)["collation"].eoo())){
                           CollationSpec spec;
                           spec.localeID = (*it)["collation"].Obj().getField("locale").str();
                           spec.version = "57.1";
                           in.append("collation",spec.toBSON());;
                       }
                       indexArrayBuilder.append(in.obj());
                       isAppend = true;
                    }
                }
                break;
            }      
        }
        if( !isAppend ){
           indexArrayBuilder.append(existingIndex);
        }
    }
    // insert the new index spec
    BSONObjBuilder indexBuilder;
    for (auto val : indexSpecs)
    {
        BSONObjBuilder indexBuilder;
        BSONObj obj;
        if( !val["collation"].eoo()){
             indexBuilder.appendElements(val.removeField("collation")); 
        }else{
             indexBuilder.appendElements(val);
        }
        indexBuilder.append ("prefix", ++ll_max_prefix);
        if(!val["collation"].eoo()){
            CollationSpec spec;
            spec.localeID = val["collation"].Obj().getField("locale").str();
            spec.version = "57.1";
            if(!val["collation"].Obj().getField("strength").eoo()){
                int n = val["collation"].Obj().getField("strength").numberInt();
                switch(n){
                   case 1:
                      spec.strength = CollationSpec::StrengthType::kPrimary;
                      break;
                   case 2:
                      spec.strength = CollationSpec::StrengthType::kSecondary;
                      break;
                   case 3:
                      spec.strength = CollationSpec::StrengthType::kTertiary;
                      break;
                   case 4:
                      spec.strength = CollationSpec::StrengthType::kQuaternary;
                      break;
                   case 5:
                      spec.strength = CollationSpec::StrengthType::kIdentical;
                      break;
                }
            }
            BSONObj obj = spec.toBSON();
            indexBuilder.append("collation",obj);
        }
        obj = indexBuilder.obj();
        toCreateCmdBuilder.append(obj);
        if(IndexDescriptor::isIdIndexPattern(obj["key"].Obj())){
              BSONObj option = coll.getOptions();
              if( !option.isEmpty() ){
                   if( !option["autoIndexId"].eoo() && !option["autoIndexId"].boolean()){
                        option = option.removeField("autoIndexId");
                        coll.setOptions(option);
                   }
              }
              continue;
        }
        auto ele = val.getField("dropDups");
        if( !ele.eoo()){
            obj = obj.removeField("dropDups");
        }
        indexArrayBuilder.append(obj);
    }


    BSONArray temp = indexArrayBuilder.arr();
    coll.setIndex(temp);
    coll.setPrefix(ll_max_prefix);
    log()<< "coll.getIndex :"<< coll.getIndex().toString();
    // TODO: need to gurantee the atomicity ...
    std::string s = cmdObj.getField("createIndexes").String();
    BSONArray ba;
    if( indexSpecs.size() == 0){
        ba = temp;
    }else{
        ba= toCreateCmdBuilder.arr();
    }
    Status st=passCreateIndexCMDtoEveryChunk(txn,ns,ba,s,dbname,subobj);
    if( !st.isOK() )
        return st;
    uassertStatusOK(grid.catalogClient(txn)->updateCollection(txn, ns.ns(), coll));
    return Status::OK();
}

MONGO_FP_DECLARE(crashAfterStartingIndexBuild);
MONGO_FP_DECLARE(hangAfterStartingIndexBuild);
MONGO_FP_DECLARE(hangAfterStartingIndexBuildUnlocked);

std::atomic<std::int32_t> maxIndexBuildMemoryUsageMegabytes(500);  // NOLINT

class ExportedMaxIndexBuildMemoryUsageParameter
    : public ExportedServerParameter<std::int32_t, ServerParameterType::kStartupAndRuntime> {
public:
    ExportedMaxIndexBuildMemoryUsageParameter()
        : ExportedServerParameter<std::int32_t, ServerParameterType::kStartupAndRuntime>(
              ServerParameterSet::getGlobal(),
              "maxIndexBuildMemoryUsageMegabytes",
              &maxIndexBuildMemoryUsageMegabytes) {}

    virtual Status validate(const std::int32_t& potentialNewValue) {
        if (potentialNewValue < 100) {
            return Status(
                ErrorCodes::BadValue,
                "maxIndexBuildMemoryUsageMegabytes must be greater than or equal to 100 MB");
        }

        return Status::OK();
    }

} exportedMaxIndexBuildMemoryUsageParameter;


/**
 * On rollback sets MultiIndexBlock::_needToCleanup to true.
 */
class MultiIndexBlock::SetNeedToCleanupOnRollback : public RecoveryUnit::Change {
public:
    explicit SetNeedToCleanupOnRollback(MultiIndexBlock* indexer) : _indexer(indexer) {}

    virtual void commit() {}
    virtual void rollback() {
        _indexer->_needToCleanup = true;
    }

private:
    MultiIndexBlock* const _indexer;
};

/**
 * On rollback in init(), cleans up _indexes so that ~MultiIndexBlock doesn't try to clean
 * up _indexes manually (since the changes were already rolled back).
 * Due to this, it is thus legal to call init() again after it fails.
 */
class MultiIndexBlock::CleanupIndexesVectorOnRollback : public RecoveryUnit::Change {
public:
    explicit CleanupIndexesVectorOnRollback(MultiIndexBlock* indexer) : _indexer(indexer) {}

    virtual void commit() {}
    virtual void rollback() {
        _indexer->_indexes.clear();
    }

private:
    MultiIndexBlock* const _indexer;
};

MultiIndexBlock::MultiIndexBlock(OperationContext* txn, Collection* collection)
    : _collection(collection),
      _txn(txn),
      _buildInBackground(false),
      _allowInterruption(false),
      _ignoreUnique(false),
      _needToCleanup(true) {}

MultiIndexBlock::~MultiIndexBlock() {
    if (!_needToCleanup || _indexes.empty())
        return;
    while (true) {
        try {
            WriteUnitOfWork wunit(_txn);
            // This cleans up all index builds.
            // Because that may need to write, it is done inside
            // of a WUOW. Nothing inside this block can fail, and it is made fatal if it does.
            for (size_t i = 0; i < _indexes.size(); i++) {
                _indexes[i].block->fail();
            }
            wunit.commit();
            return;
        } catch (const WriteConflictException& e) {
            continue;
        } catch (const DBException& e) {
            if (e.toStatus() == ErrorCodes::ExceededMemoryLimit)
                continue;
            error() << "Caught exception while cleaning up partially built indexes: " << redact(e);
        } catch (const std::exception& e) {
            error() << "Caught exception while cleaning up partially built indexes: " << e.what();
        } catch (...) {
            error() << "Caught unknown exception while cleaning up partially built indexes.";
        }
        fassertFailed(18644);
    }
}

void MultiIndexBlock::removeExistingIndexes(std::vector<BSONObj>* specs) const {
    for (size_t i = 0; i < specs->size(); i++) {
        Status status =
            _collection->getIndexCatalog()->prepareSpecForCreate(_txn, (*specs)[i]).getStatus();
        if (status.code() == ErrorCodes::IndexAlreadyExists) {
            specs->erase(specs->begin() + i);
            i--;
        }
        // intentionally ignoring other error codes
    }
}

StatusWith<std::vector<BSONObj>> MultiIndexBlock::init(const BSONObj& spec) {
    const auto indexes = std::vector<BSONObj>(1, spec);
    return init(indexes);
}

StatusWith<std::vector<BSONObj>> MultiIndexBlock::init(const std::vector<BSONObj>& indexSpecs) {
    WriteUnitOfWork wunit(_txn);

    invariant(_indexes.empty());
    _txn->recoveryUnit()->registerChange(new CleanupIndexesVectorOnRollback(this));

    const string& ns = _collection->ns().ns();

    const auto idxCat = _collection->getIndexCatalog();
    invariant(idxCat);
    invariant(idxCat->ok());
    Status status = idxCat->checkUnfinished();
    if (!status.isOK())
        return status;

    for (size_t i = 0; i < indexSpecs.size(); i++) {
        BSONObj info = indexSpecs[i].getOwned();
        string pluginName = IndexNames::findPluginName(info["key"].Obj());
        if (pluginName.size()) {
            Status s = _collection->getIndexCatalog()->_upgradeDatabaseMinorVersionIfNeeded(
                _txn, pluginName);
            if (!s.isOK())
                return s;
        }

        // Any foreground indexes make all indexes be built in the foreground.
        _buildInBackground = (_buildInBackground && info["background"].trueValue());
    }

    std::vector<BSONObj> indexInfoObjs;
    indexInfoObjs.reserve(indexSpecs.size());
    std::size_t eachIndexBuildMaxMemoryUsageBytes = 0;
    if (!indexSpecs.empty()) {
        eachIndexBuildMaxMemoryUsageBytes =
            std::size_t(maxIndexBuildMemoryUsageMegabytes) * 1024 * 1024 / indexSpecs.size();
    }

    for (size_t i = 0; i < indexSpecs.size(); i++) {
        BSONObj info = indexSpecs[i].getOwned();

        if (!info.hasField("prefix")) {
            index_log() << "[createIndex] MultiIndexBlock::init() origin info: " << info;
            NamespaceString nss = _collection->ns();
            if (GLOBAL_CONFIG_GET(IsCoreTest) && !nss.isSystemCollection()) {
                  static long long prefix = 10000;
                  BSONObjBuilder builder;

                  BSONObjIterator it(info);
                  while (it.more()) {
                      builder.append(*it);
                      it++;
                  }

                  builder.append("prefix", prefix++);
                  info = builder.obj();
                  index_log() << "[createIndex] MultiIndexBlock::init() fixup info: " << info;
            }
        }

        StatusWith<BSONObj> statusWithInfo =
            _collection->getIndexCatalog()->prepareSpecForCreate(_txn, info);
        Status status = statusWithInfo.getStatus();
        if (!status.isOK())
            return status;
        info = statusWithInfo.getValue();
        indexInfoObjs.push_back(info);

        IndexToBuild index;
        index.block.reset(new IndexCatalog::IndexBuildBlock(_txn, _collection, info));
        status = index.block->init();
        if (!status.isOK())
            return status;

        index.real = index.block->getEntry()->accessMethod();
        status = index.real->initializeAsEmpty(_txn);
        if (!status.isOK())
            return status;

        if (!_buildInBackground) {
            // Bulk build process requires foreground building as it assumes nothing is changing
            // under it.
            index.bulk = index.real->initiateBulk(eachIndexBuildMaxMemoryUsageBytes);
        }

        const IndexDescriptor* descriptor = index.block->getEntry()->descriptor();

        IndexCatalog::prepareInsertDeleteOptions(_txn, descriptor, &index.options);
        index.options.dupsAllowed = index.options.dupsAllowed || _ignoreUnique;
        if (_ignoreUnique) {
            index.options.getKeysMode = IndexAccessMethod::GetKeysMode::kRelaxConstraints;
        }

        log() << "build index on: " << ns << " properties: " << descriptor->toString();
        if (index.bulk)
            log() << "\t building index using bulk method; build may temporarily use up to "
                  << eachIndexBuildMaxMemoryUsageBytes / 1024 / 1024 << " megabytes of RAM";

        index.filterExpression = index.block->getEntry()->getFilterExpression();

        // TODO SERVER-14888 Suppress this in cases we don't want to audit.
        audit::logCreateIndex(_txn->getClient(), &info, descriptor->indexName(), ns);

        _indexes.push_back(std::move(index));
    }

    if (_buildInBackground)
        _backgroundOperation.reset(new BackgroundOperation(ns));

    wunit.commit();

    if (MONGO_FAIL_POINT(crashAfterStartingIndexBuild)) {
        log() << "Index build interrupted due to 'crashAfterStartingIndexBuild' failpoint. Exiting "
                 "after waiting for changes to become durable.";
        Locker::LockSnapshot lockInfo;
        _txn->lockState()->saveLockStateAndUnlock(&lockInfo);
        if (_txn->recoveryUnit()->waitUntilDurable()) {
            quickExit(EXIT_TEST);
        }
    }

    return indexInfoObjs;
}

Status MultiIndexBlock::insertAllDocumentsInCollection(std::set<RecordId>* dupsOut) {
    const char* curopMessage = _buildInBackground ? "Index Build (background)" : "Index Build";
    const auto numRecords = _collection->numRecords(_txn);
    stdx::unique_lock<Client> lk(*_txn->getClient());
    ProgressMeterHolder progress(*_txn->setMessage_inlock(curopMessage, curopMessage, numRecords));
    lk.unlock();

    Timer t;

    unsigned long long n = 0;

    unique_ptr<PlanExecutor> exec(InternalPlanner::collectionScan(
        _txn, _collection->ns().ns(), _collection, PlanExecutor::YIELD_MANUAL));
    if (_buildInBackground) {
        invariant(_allowInterruption);
        exec->setYieldPolicy(PlanExecutor::YIELD_AUTO, _collection);
    } else {
        exec->setYieldPolicy(PlanExecutor::WRITE_CONFLICT_RETRY_ONLY, _collection);
    }

    Snapshotted<BSONObj> objToIndex;
    RecordId loc;
    PlanExecutor::ExecState state;
    int retries = 0;  // non-zero when retrying our last document.
    while (retries ||
           (PlanExecutor::ADVANCED == (state = exec->getNextSnapshotted(&objToIndex, &loc)))) {
        try {
            if (_allowInterruption)
                _txn->checkForInterrupt();

            // Make sure we are working with the latest version of the document.
            if (objToIndex.snapshotId() != _txn->recoveryUnit()->getSnapshotId() &&
                !_collection->findDoc(_txn, loc, &objToIndex)) {
                // doc was deleted so don't index it.
                retries = 0;
                continue;
            }

            // Done before insert so we can retry document if it WCEs.
            progress->setTotalWhileRunning(_collection->numRecords(_txn));

            WriteUnitOfWork wunit(_txn);
            Status ret = insert(objToIndex.value(), loc);
            if (_buildInBackground)
                exec->saveState();
            if (ret.isOK()) {
                wunit.commit();
            } else if (dupsOut && ret.code() == ErrorCodes::DuplicateKey) {
                // If dupsOut is non-null, we should only fail the specific insert that
                // led to a DuplicateKey rather than the whole index build.
                dupsOut->insert(loc);
            } else {
                // Fail the index build hard.
                return ret;
            }
            if (_buildInBackground)
                exec->restoreState();  // Handles any WCEs internally.

            // Go to the next document
            progress->hit();
            n++;
            retries = 0;
        } catch (const WriteConflictException& wce) {
            CurOp::get(_txn)->debug().writeConflicts++;
            retries++;  // logAndBackoff expects this to be 1 on first call.
            wce.logAndBackoff(retries, "index creation", _collection->ns().ns());

            // Can't use WRITE_CONFLICT_RETRY_LOOP macros since we need to save/restore exec
            // around call to abandonSnapshot.
            exec->saveState();
            _txn->recoveryUnit()->abandonSnapshot();
            exec->restoreState();  // Handles any WCEs internally.
        }
    }

    uassert(28550,
            "Unable to complete index build due to collection scan failure: " +
                WorkingSetCommon::toStatusString(objToIndex.value()),
            state == PlanExecutor::IS_EOF);

    if (MONGO_FAIL_POINT(hangAfterStartingIndexBuild)) {
        // Need the index build to hang before the progress meter is marked as finished so we can
        // reliably check that the index build has actually started in js tests.
        while (MONGO_FAIL_POINT(hangAfterStartingIndexBuild)) {
            log() << "Hanging index build due to 'hangAfterStartingIndexBuild' failpoint";
            sleepmillis(1000);
        }

        // Check for interrupt to allow for killop prior to index build completion.
        _txn->checkForInterrupt();
    }

    if (MONGO_FAIL_POINT(hangAfterStartingIndexBuildUnlocked)) {
        // Unlock before hanging so replication recognizes we've completed.
        Locker::LockSnapshot lockInfo;
        _txn->lockState()->saveLockStateAndUnlock(&lockInfo);
        while (MONGO_FAIL_POINT(hangAfterStartingIndexBuildUnlocked)) {
            log() << "Hanging index build with no locks due to "
                     "'hangAfterStartingIndexBuildUnlocked' failpoint";
            sleepmillis(1000);
        }
        // If we want to support this, we'd need to regrab the lock and be sure that all callers are
        // ok with us yielding. They should be for BG indexes, but not for foreground.
        invariant(!"the hangAfterStartingIndexBuildUnlocked failpoint can't be turned off");
    }

    progress->finished();

    Status ret = doneInserting(dupsOut);
    if (!ret.isOK())
        return ret;

    log() << "build index done.  scanned " << n << " total records. " << t.seconds() << " secs";

    return Status::OK();
}

Status MultiIndexBlock::insert(const BSONObj& doc, const RecordId& loc) {
    for (size_t i = 0; i < _indexes.size(); i++) {
        if (_indexes[i].filterExpression && !_indexes[i].filterExpression->matchesBSON(doc)) {
            continue;
        }

        int64_t unused;
        Status idxStatus(ErrorCodes::InternalError, "");
        if (_indexes[i].bulk) {
            idxStatus = _indexes[i].bulk->insert(_txn, doc, loc, _indexes[i].options, &unused);
        } else {
            idxStatus = _indexes[i].real->insert(_txn, doc, loc, _indexes[i].options, &unused);
        }

        if (!idxStatus.isOK())
            return idxStatus;
    }
    return Status::OK();
}

Status MultiIndexBlock::doneInserting(std::set<RecordId>* dupsOut) {
    for (size_t i = 0; i < _indexes.size(); i++) {
        if (_indexes[i].bulk == NULL)
            continue;
        LOG(1) << "\t bulk commit starting for index: "
               << _indexes[i].block->getEntry()->descriptor()->indexName();
        Status status = _indexes[i].real->commitBulk(_txn,
                                                     std::move(_indexes[i].bulk),
                                                     _allowInterruption,
                                                     _indexes[i].options.dupsAllowed,
                                                     dupsOut);
        if (!status.isOK()) {
            return status;
        }
    }

    return Status::OK();
}

void MultiIndexBlock::abortWithoutCleanup() {
    _indexes.clear();
    _needToCleanup = false;
}

void MultiIndexBlock::commit() {
    for (size_t i = 0; i < _indexes.size(); i++) {
        _indexes[i].block->success();
    }

    _txn->recoveryUnit()->registerChange(new SetNeedToCleanupOnRollback(this));
    _needToCleanup = false;
}

}  // namespace mongo
