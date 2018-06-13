// database.cpp

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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "mongo/db/catalog/database.h"

#include <algorithm>
#include <boost/filesystem/operations.hpp>
#include <memory>

#include "mongo/db/audit.h"
#include "mongo/db/auth/auth_index_d.h"
#include "mongo/db/background.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/collection_catalog_entry.h"
#include "mongo/db/catalog/collection_options.h"
#include "mongo/db/catalog/database_catalog_entry.h"
#include "mongo/db/catalog/database_holder.h"
#include "mongo/db/catalog/index_create.h"
#include "mongo/db/clientcursor.h"
#include "mongo/db/concurrency/write_conflict_exception.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/index/index_access_method.h"
#include "mongo/db/instance.h"
#include "mongo/db/introspect.h"
#include "mongo/db/op_observer.h"
#include "mongo/db/query/collation/collator_factory_interface.h"
#include "mongo/db/repl/oplog.h"
#include "mongo/db/repl/replication_coordinator_global.h"
#include "mongo/db/server_options.h"
#include "mongo/db/server_parameters.h"
#include "mongo/db/service_context.h"
#include "mongo/db/service_context_d.h"
#include "mongo/db/stats/top.h"
#include "mongo/db/storage/recovery_unit.h"
#include "mongo/db/storage/storage_engine.h"
#include "mongo/db/storage/storage_options.h"
#include "mongo/db/views/view_catalog.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"
#include "mongo/db/modules/rocks/src/Chunk/ChunkRocksRecordStore.h"





namespace mongo {

using std::unique_ptr;
using std::endl;
using std::list;
using std::set;
using std::string;
using std::stringstream;
using std::vector;

extern StatusWith<std::vector<BSONObj>> parseAndValidateIndexSpecs(
    const NamespaceString& ns,
    const BSONObj& cmdObj,
    const ServerGlobalParams::FeatureCompatibility& featureCompatibility);

extern StatusWith<std::vector<BSONObj>> resolveCollectionDefaultProperties(
    OperationContext* txn,
    const Collection* collection, 
    std::vector<BSONObj> indexSpecs);

void massertNamespaceNotIndex(StringData ns, StringData caller) {
    massert(17320,
            str::stream() << "cannot do " << caller << " on namespace with a $ in it: " << ns,
            NamespaceString::normal(ns));
}

class Database::AddCollectionChange : public RecoveryUnit::Change {
public:
    AddCollectionChange(OperationContext* txn, Database* db, StringData ns)
        : _txn(txn), _db(db), _ns(ns.toString()) {}

    virtual void commit() {
        stdx::lock_guard<stdx::mutex> lock(_db->_collectionsMutex);
        CollectionMap::const_iterator it = _db->_collections.find(_ns);
        if (it == _db->_collections.end())
            return;

        // Ban reading from this collection on committed reads on snapshots before now.
        auto replCoord = repl::ReplicationCoordinator::get(_txn);
        auto snapshotName = replCoord->reserveSnapshotName(_txn);
        replCoord->forceSnapshotCreation();  // Ensures a newer snapshot gets created even if idle.
        it->second->setMinimumVisibleSnapshot(snapshotName);
    }

    virtual void rollback() {
        stdx::lock_guard<stdx::mutex> lock(_db->_collectionsMutex);
        CollectionMap::const_iterator it = _db->_collections.find(_ns);
        if (it == _db->_collections.end())
            return;

        delete it->second;
        _db->_collections.erase(it);
    }

    OperationContext* const _txn;
    Database* const _db;
    const std::string _ns;
};

class Database::RemoveCollectionChange : public RecoveryUnit::Change {
public:
    // Takes ownership of coll (but not db).
    RemoveCollectionChange(Database* db, Collection* coll) : _db(db), _coll(coll) {}

    virtual void commit() {
        delete _coll;
    }

    virtual void rollback() {
        stdx::lock_guard<stdx::mutex> lock(_db->_collectionsMutex);
        Collection*& inMap = _db->_collections[_coll->ns().ns()];
        invariant(!inMap);
        inMap = _coll;
    }

    Database* const _db;
    Collection* const _coll;
};

Database::~Database() {
    stdx::lock_guard<stdx::mutex> lk(_collectionsMutex);
    for (CollectionMap::const_iterator i = _collections.begin(); i != _collections.end(); ++i)
        delete i->second;
}

void Database::close(OperationContext* txn) {
    // XXX? - Do we need to close database under global lock or just DB-lock is sufficient ?
    invariant(txn->lockState()->isW());
    // oplog caches some things, dirty its caches
    repl::oplogCheckCloseDatabase(txn, this);

    if (BackgroundOperation::inProgForDb(_name)) {
        log() << "warning: bg op in prog during close db? " << _name;
    }
}

Status Database::validateDBName(StringData dbname) {
    if (dbname.size() <= 0)
        return Status(ErrorCodes::BadValue, "db name is empty");

    if (dbname.size() >= 64)
        return Status(ErrorCodes::BadValue, "db name is too long");

    if (dbname.find('.') != string::npos)
        return Status(ErrorCodes::BadValue, "db name cannot contain a .");

    if (dbname.find(' ') != string::npos)
        return Status(ErrorCodes::BadValue, "db name cannot contain a space");

#ifdef _WIN32
    static const char* windowsReservedNames[] = {
        "con",  "prn",  "aux",  "nul",  "com1", "com2", "com3", "com4", "com5", "com6", "com7",
        "com8", "com9", "lpt1", "lpt2", "lpt3", "lpt4", "lpt5", "lpt6", "lpt7", "lpt8", "lpt9"};

    string lower(dbname.toString());
    std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
    for (size_t i = 0; i < (sizeof(windowsReservedNames) / sizeof(char*)); ++i) {
        if (lower == windowsReservedNames[i]) {
            stringstream errorString;
            errorString << "db name \"" << dbname.toString() << "\" is a reserved name";
            return Status(ErrorCodes::BadValue, errorString.str());
        }
    }
#endif

    return Status::OK();
}

Collection* Database::_getOrCreateCollectionInstance(OperationContext* txn, StringData fullns) {
    Collection* collection = getCollection(fullns);
    if (collection) {
        return collection;
    }

    unique_ptr<CollectionCatalogEntry> cce(_dbEntry->getCollectionCatalogEntry(fullns));
    invariant(cce.get());

    unique_ptr<RecordStore> rs(_dbEntry->getRecordStore(fullns));
    invariant(rs.get());  // if cce exists, so should this

    // Not registering AddCollectionChange since this is for collections that already exist.
    Collection* c = new Collection(txn, fullns, cce.release(), rs.release(), _dbEntry);
    return c;
}

Database::Database(OperationContext* txn, StringData name, DatabaseCatalogEntry* dbEntry)
    : _name(name.toString()),
      _dbEntry(dbEntry),
      _profileName(_name + ".system.profile"),
      _indexesName(_name + ".system.indexes"),
      _viewsName(_name + "." + DurableViewCatalog::viewsCollectionName().toString()),
      _durableViews(DurableViewCatalogImpl(this)),
      _views(&_durableViews) {
    Status status = validateDBName(_name);
    if (!status.isOK()) {
        warning() << "tried to open invalid db: " << _name;
        uasserted(10028, status.toString());
    }

    _profile = serverGlobalParams.defaultProfile;

    list<string> collections;
    _dbEntry->getCollectionNamespaces(&collections);
    for (list<string>::const_iterator it = collections.begin(); it != collections.end(); ++it) {
        const string ns = *it;
        Collection* coll = _getOrCreateCollectionInstance(txn, ns);
        stdx::lock_guard<stdx::mutex> lk(_collectionsMutex);
        _collections[ns] = coll;
    }
    // At construction time of the viewCatalog, the _collections map wasn't initialized yet, so no
    // system.views collection would be found. Now we're sufficiently initialized, signal a version
    // change. Also force a reload, so if there are problems with the catalog contents as might be
    // caused by incorrect mongod versions or similar, they are found right away.
    _views.invalidate();
    Status reloadStatus = _views.reloadIfNeeded(txn);
    if (!reloadStatus.isOK()) {
        warning() << "Unable to parse views: " << redact(reloadStatus)
                  << "; remove any invalid views from the " << _viewsName
                  << " collection to restore server functionality." << startupWarningsLog;
    }
}

/*static*/
string Database::duplicateUncasedName(const string& name, set<string>* duplicates) {
    if (duplicates) {
        duplicates->clear();
    }

    set<string> allShortNames;
    dbHolder().getAllShortNames(allShortNames);

    for (const auto& dbname : allShortNames) {
        if (strcasecmp(dbname.c_str(), name.c_str()))
            continue;

        if (strcmp(dbname.c_str(), name.c_str()) == 0)
            continue;

        if (duplicates) {
            duplicates->insert(dbname);
        } else {
            return dbname;
        }
    }
    if (duplicates) {
        return duplicates->empty() ? "" : *duplicates->begin();
    }
    return "";
}

void Database::clearTmpCollections(OperationContext* txn) {
    invariant(txn->lockState()->isDbLockedForMode(name(), MODE_X));

    list<string> collections;
    _dbEntry->getCollectionNamespaces(&collections);

    for (list<string>::iterator i = collections.begin(); i != collections.end(); ++i) {
        string ns = *i;
        //invariant(NamespaceString::normal(ns)); //chunk collection include '$' will be thougt as virtual collection.

        CollectionCatalogEntry* coll = _dbEntry->getCollectionCatalogEntry(ns);

        CollectionOptions options = coll->getCollectionOptions(txn);
        if (!options.temp)
            continue;
        try {
            WriteUnitOfWork wunit(txn);
            Status status = dropCollection(txn, ns);
            if (!status.isOK()) {
                warning() << "could not drop temp collection '" << ns << "': " << redact(status);
                continue;
            }

            wunit.commit();
        } catch (const WriteConflictException& exp) {
            warning() << "could not drop temp collection '" << ns << "' due to "
                                                                     "WriteConflictException";
            txn->recoveryUnit()->abandonSnapshot();
        }
    }
}

Status Database::setProfilingLevel(OperationContext* txn, int newLevel) {
    if (_profile == newLevel) {
        return Status::OK();
    }

    if (newLevel == 0) {
        _profile = 0;
        return Status::OK();
    }

    if (newLevel < 0 || newLevel > 2) {
        return Status(ErrorCodes::BadValue, "profiling level has to be >=0 and <= 2");
    }

    Status status = createProfileCollection(txn, this);
    if (!status.isOK()) {
        return status;
    }

    _profile = newLevel;

    return Status::OK();
}

void Database::getStats(OperationContext* opCtx, BSONObjBuilder* output, double scale) {
    list<string> collections;
    _dbEntry->getCollectionNamespaces(&collections);

    long long nCollections = 0;
    long long nViews = 0;
    long long objects = 0;
    long long size = 0;
    long long storageSize = 0;
    long long numExtents = 0;
    long long indexes = 0;
    long long indexSize = 0;

    for (list<string>::const_iterator it = collections.begin(); it != collections.end(); ++it) {
        const string ns = *it;

        Collection* collection = getCollection(ns);
        if (!collection)
            continue;

        nCollections += 1;
        objects += collection->numRecords(opCtx);
        size += collection->dataSize(opCtx);

        BSONObjBuilder temp;
        storageSize += collection->getRecordStore()->storageSize(opCtx, &temp);
        numExtents += temp.obj()["numExtents"].numberInt();  // XXX

        indexes += collection->getIndexCatalog()->numIndexesTotal(opCtx);
        indexSize += collection->getIndexSize(opCtx);
    }

    getViewCatalog()->iterate(opCtx, [&](const ViewDefinition& view) { nViews += 1; });

    output->appendNumber("collections", nCollections);
    output->appendNumber("views", nViews);
    output->appendNumber("objects", objects);
    output->append("avgObjSize", objects == 0 ? 0 : double(size) / double(objects));
    output->appendNumber("dataSize", size / scale);
    output->appendNumber("storageSize", storageSize / scale);
    output->appendNumber("numExtents", numExtents);
    output->appendNumber("indexes", indexes);
    output->appendNumber("indexSize", indexSize / scale);

    _dbEntry->appendExtraStats(opCtx, output, scale);
}

Status Database::dropView(OperationContext* txn, StringData fullns) {
    Status status = _views.dropView(txn, NamespaceString(fullns));
    Top::get(txn->getClient()->getServiceContext()).collectionDropped(fullns);
    return status;
}

Status Database::dropCollection(OperationContext* txn, StringData fullns) {
    invariant(txn->lockState()->isDbLockedForMode(name(), MODE_IX));
    invariant(txn->lockState()->isCollectionLockedForMode(fullns, MODE_X));

    LOG(1) << "dropCollection: " << fullns;
    //massertNamespaceNotIndex(fullns, "dropCollection");

    Collection* collection = getCollection(fullns);
    if (!collection) {
        // collection doesn't exist
        return Status::OK();
    }

    if (collection){
        //wait until assign is done
        if (collection->isAssigning()){
            LOG (1) << "wait chunk to be assigned completely:" << fullns ;
            throw WriteConflictException();

        }    
    }

    NamespaceString nss(fullns);
    {
        verify(nss.db() == _name);
        if (nss.isSystem()) {
            size_t pos = nss.ns().find('$');
            bool withChunkId = (pos != std::string::npos);
            NamespaceString nssWithoutChunkId(nss);
            if (withChunkId) {
               nssWithoutChunkId = NamespaceString(StringData(nss.ns().substr(0, pos))); 
            }
            if (nssWithoutChunkId.isSystemDotProfile()) {
                if (_profile != 0)
                    return Status(ErrorCodes::IllegalOperation,
                                  "turn off profiling before dropping system.profile collection");
            }
            else if (nssWithoutChunkId.isSystemDotUsers()) {
                if (nss.isOnInternalDb()) {
                    return Status(ErrorCodes::IllegalOperation, "can't drop system ns");
                }
            }
            else if (!nssWithoutChunkId.isSystemDotViews()) {
                return Status(ErrorCodes::IllegalOperation, "can't drop system ns");
            }
        }
    }

    BackgroundOperation::assertNoBgOpInProgForNs(fullns);

    audit::logDropCollection(&cc(), fullns);

    Status s = collection->getIndexCatalog()->dropAllIndexes(txn, true);
    if (!s.isOK()) {
        warning() << "could not drop collection, trying to drop indexes" << fullns << " because of "
                  << redact(s.toString());
        return s;
    }

    verify(collection->_details->getTotalIndexCount(txn) == 0);
    LOG(1) << "\t dropIndexes done";

    Top::get(txn->getClient()->getServiceContext()).collectionDropped(fullns);

    // We want to destroy the Collection object before telling the StorageEngine to destroy the
    // RecordStore.
    _clearCollectionCache(txn, fullns, "collection dropped");

    s = _dbEntry->dropCollection(txn, fullns);

    if (!s.isOK())
        return s;

    DEV {
        // check all index collection entries are gone
        string nstocheck = fullns.toString() + ".$";
        stdx::lock_guard<stdx::mutex> lk(_collectionsMutex);
        for (CollectionMap::const_iterator i = _collections.begin(); i != _collections.end(); ++i) {
            string temp = i->first;
            if (temp.find(nstocheck) != 0)
                continue;
            log() << "after drop, bad cache entries for: " << fullns << " have " << temp;
            verify(0);
        }
    }

    auto opObserver = getGlobalServiceContext()->getOpObserver();
    if (opObserver)
        opObserver->onDropCollection(txn, nss);

    return Status::OK();
}

void Database::_clearCollectionCache(OperationContext* txn,
                                     StringData fullns,
                                     const std::string& reason) {
    verify(_name == nsToDatabaseSubstring(fullns));
    stdx::lock_guard<stdx::mutex> lk(_collectionsMutex);
    CollectionMap::const_iterator it = _collections.find(fullns.toString());
    if (it == _collections.end())
        return;

    // Takes ownership of the collection
    txn->recoveryUnit()->registerChange(new RemoveCollectionChange(this, it->second));

    it->second->_cursorManager.invalidateAll(false, reason);
    _collections.erase(it);
}

Collection* Database::getCollection(StringData ns, bool including_assigning) {
    invariant(_name == nsToDatabaseSubstring(ns));
    stdx::lock_guard<stdx::mutex> lk(_collectionsMutex);
    CollectionMap::const_iterator it = _collections.find(ns);
    if (it != _collections.end() && it->second ) {
      
        //ingore the chunk that is in assigning
        if (!including_assigning && it->second->isAssigning()){
            log()<<"coll is assigning "<<ns<<" "<<reinterpret_cast<unsigned long long>(this);
            return NULL;
        }
        
        return it->second;
    }

    return NULL;
}

void Database::listCollections(std::vector<Collection*>& out) const {
    stdx::lock_guard<stdx::mutex> lk(_collectionsMutex);    
    for (const auto& entry : _collections){
        if (!entry.second->isAssigning()){
            out.push_back(entry.second);
        }    
    }    
}

void Database::listCollectionNSs(std::vector<NamespaceString>& out) const {
    stdx::lock_guard<stdx::mutex> lk(_collectionsMutex);    
    for (const auto& entry : _collections){
        if (!entry.second->isAssigning()){
            out.push_back(entry.second->ns());
        }    
    }    
}
Status Database::renameCollection(OperationContext* txn,
                                  StringData fromNS,
                                  StringData toNS,
                                  bool stayTemp) {
    audit::logRenameCollection(&cc(), fromNS, toNS);
    invariant(txn->lockState()->isDbLockedForMode(name(), MODE_IX));
    invariant(txn->lockState()->isCollectionLockedForMode(fromNS, MODE_X));
    invariant(txn->lockState()->isCollectionLockedForMode(toNS, MODE_X));
    BackgroundOperation::assertNoBgOpInProgForNs(fromNS);
    BackgroundOperation::assertNoBgOpInProgForNs(toNS);

    {  // remove anything cached
        Collection* coll = getCollection(fromNS);
        if (!coll)
            return Status(ErrorCodes::NamespaceNotFound, "collection not found to rename");

        string clearCacheReason = str::stream() << "renamed collection '" << fromNS << "' to '"
                                                << toNS << "'";
        IndexCatalog::IndexIterator ii = coll->getIndexCatalog()->getIndexIterator(txn, true);
        while (ii.more()) {
            IndexDescriptor* desc = ii.next();
            _clearCollectionCache(txn, desc->indexNamespace(), clearCacheReason);
        }

        _clearCollectionCache(txn, fromNS, clearCacheReason);
        _clearCollectionCache(txn, toNS, clearCacheReason);

        Top::get(txn->getClient()->getServiceContext()).collectionDropped(fromNS.toString());
    }

    txn->recoveryUnit()->registerChange(new AddCollectionChange(txn, this, toNS));
    Status s = _dbEntry->renameCollection(txn, fromNS, toNS, stayTemp);
    Collection* newColl = _getOrCreateCollectionInstance(txn, toNS);
    stdx::lock_guard<stdx::mutex> lk(_collectionsMutex);
    _collections[toNS] = newColl;
    return s;
}

Collection* Database::getOrCreateCollection(OperationContext* txn, StringData ns) {
    Collection* c = getCollection(ns);
    if (!c) {
        c = createCollection(txn, ns);
    }
    return c;
}

void Database::_checkCanCreateCollection(const NamespaceString& nss,
                                         const CollectionOptions& options) {
    massert(17399, "collection already exists", getCollection(nss.ns()) == nullptr);
    //massertNamespaceNotIndex(nss.ns(), "createCollection");

    uassert(14037,
            "can't create user databases on a --configsvr instance",
            serverGlobalParams.clusterRole != ClusterRole::ConfigServer || nss.isOnInternalDb());

    // This check only applies for actual collections, not indexes or other types of ns.
    uassert(17381,
            str::stream() << "fully qualified namespace " << nss.ns() << " is too long "
                          << "(max is "
                          << NamespaceString::MaxNsCollectionLen
                          << " bytes)",
            !nss.isNormal() || nss.size() <= NamespaceString::MaxNsCollectionLen);
    uassert(17316, "cannot create a blank collection", nss.coll() > 0);
    uassert(28838, "cannot create a non-capped oplog collection", options.capped || !nss.isOplog());
}

Status Database::createView(OperationContext* txn,
                            StringData ns,
                            const CollectionOptions& options) {
    invariant(txn->lockState()->isDbLockedForMode(name(), MODE_IX));
    invariant(txn->lockState()->isCollectionLockedForMode(ns, MODE_X));
    invariant(options.isView());

    NamespaceString nss(ns);
    NamespaceString viewOnNss(nss.db(), options.viewOn);
    _checkCanCreateCollection(nss, options);
    audit::logCreateCollection(&cc(), ns);

    if (nss.isOplog())
        return Status(ErrorCodes::InvalidNamespace,
                      str::stream() << "invalid namespace name for a view: " + nss.toString());

    return _views.createView(txn, nss, viewOnNss, BSONArray(options.pipeline), options.collation);
}


Collection* Database::createCollection(OperationContext* txn,
                                       StringData ns,
                                       const CollectionOptions& options,
                                       bool createIdIndex,
                                       const BSONObj& idIndex) {
    NamespaceString nss(ns);
    index_log() << "[createCollection] Database::createCollection ns: " << ns << "; createIdIndex: " <<
        createIdIndex << "; idIndex: " << idIndex << "; options: " << options.toBSON();

    if (GLOBAL_CONFIG_GET(IsCoreTest) && !nss.isSystemCollection()) {
        index_log() << "[createCollection] mockAssignChunk ns: " << ns;
        mockAssignChunk(txn, ns, options, createIdIndex, idIndex);
        return  getCollection(ns, true);
    } 

    invariant(txn->lockState()->isDbLockedForMode(name(), MODE_IX));
    invariant(txn->lockState()->isCollectionLockedForMode(ns, MODE_X));
    invariant(!options.isView());

    _checkCanCreateCollection(nss, options);
    audit::logCreateCollection(&cc(), ns);
    LOG(1) << "Database::createCollection() ns:"<< ns << ", options: " << options.toBSON() 
          << "; idIndex: " << idIndex<<",createIdIndex:"<<createIdIndex;
    Status status = _dbEntry->createCollection(txn, ns, options, true /*allocateDefaultSpace*/);
    massertNoTraceStatusOK(status);

    txn->recoveryUnit()->registerChange(new AddCollectionChange(txn, this, ns));
    Collection* collection = _getOrCreateCollectionInstance(txn, ns);
    invariant(collection);
    {
        stdx::lock_guard<stdx::mutex> lk(_collectionsMutex);
        _collections[ns] = collection;
    }

    BSONObj fullIdIndexSpec;

    if (createIdIndex) {
        if (collection->requiresIdIndex()) {
            if (options.autoIndexId == CollectionOptions::YES ||
                options.autoIndexId == CollectionOptions::DEFAULT) {
                const auto featureCompatibilityVersion =
                    serverGlobalParams.featureCompatibility.version.load();
                IndexCatalog* ic = collection->getIndexCatalog();
                fullIdIndexSpec = uassertStatusOK(ic->createIndexOnEmptyCollection(
                    txn,
                    !idIndex.isEmpty() ? idIndex
                                       : ic->getDefaultIdIndexSpec(featureCompatibilityVersion)));
            }
        }

        if (nss.isSystem()) {
            authindex::createSystemIndexes(txn, collection);
        }
    }

    auto opObserver = getGlobalServiceContext()->getOpObserver();
    if (opObserver)
        opObserver->onCreateCollection(txn, nss, options, fullIdIndexSpec);

    return collection;
}

const DatabaseCatalogEntry* Database::getDatabaseCatalogEntry() const {
    return _dbEntry;
}

void dropAllDatabasesExceptLocal(OperationContext* txn) {
    ScopedTransaction transaction(txn, MODE_X);
    Lock::GlobalWrite lk(txn->lockState());

    vector<string> n;
    StorageEngine* storageEngine = getGlobalServiceContext()->getGlobalStorageEngine();
    storageEngine->listDatabases(&n);

    if (n.size() == 0)
        return;
    log() << "dropAllDatabasesExceptLocal " << n.size();

    repl::getGlobalReplicationCoordinator()->dropAllSnapshots();
    for (vector<string>::iterator i = n.begin(); i != n.end(); i++) {
        if (*i != "local") {
            MONGO_WRITE_CONFLICT_RETRY_LOOP_BEGIN {
                Database* db = dbHolder().get(txn, *i);
                // This is needed since dropDatabase can't be rolled back.
                // This is safe be replaced by "invariant(db);dropDatabase(txn, db);" once fixed
                if (db == nullptr) {
                    log() << "database disappeared after listDatabases but before drop: " << *i;
                } else {
                    Database::dropDatabase(txn, db);
                }
            }
            MONGO_WRITE_CONFLICT_RETRY_LOOP_END(txn, "dropAllDatabasesExceptLocal", *i);
        }
    }
}

void Database::dropDatabase(OperationContext* txn, Database* db) {
    invariant(db);

    // Store the name so we have if for after the db object is deleted
    const string name = db->name();
    LOG(1) << "dropDatabase " << name;

    invariant(txn->lockState()->isDbLockedForMode(name, MODE_X));

    BackgroundOperation::assertNoBgOpInProgForDb(name);

    audit::logDropDatabase(txn->getClient(), name);

    for (auto&& coll : *db) {
        Top::get(txn->getClient()->getServiceContext()).collectionDropped(coll->ns().ns(), true);
    }

    dbHolder().close(txn, name);
    db = NULL;  // d is now deleted

    MONGO_WRITE_CONFLICT_RETRY_LOOP_BEGIN {
        getGlobalServiceContext()->getGlobalStorageEngine()->dropDatabase(txn, name);
    }
    MONGO_WRITE_CONFLICT_RETRY_LOOP_END(txn, "dropDatabase", name);
}

Status Database::mockAssignChunk(OperationContext* txn, 
                                StringData ns,
                                CollectionOptions options, 
                                bool createDefaultIndexes,
                                const BSONObj& idIndex)
{
    index_log() "[assignChunk] Database::mockAssign ns: " << ns <<"; option: ( " << options.toBSON() << " ); createDefaultIndexes: "
        << createDefaultIndexes << ";idIndex: ( " << idIndex;

    static long long prefix = 3000;

    OID epoch = OID::gen();
    CollectionType collType;
    collType.setNs(NamespaceString(ns));
    collType.setKeyPattern(BSON("_id" << 1));
    collType.setUnique(true);
    collType.setUpdatedAt(Date_t::fromMillisSinceEpoch(1));
    collType.setEpoch(epoch);
    collType.setDropped(false);
    collType.setPrefix(prefix++);
    //if (auto collation = options.toBSON()["collation"]) {
    //    collType.setDefaultCollation(collation.Obj());
    //}

    if (!options.toBSON().isEmpty()) {
        collType.setOptions(options.toBSON());
    }

    if (createDefaultIndexes && !idIndex.isEmpty()) {
        index_err() << "[assignChunk] Database::mockAssign idindex not isempty";
        BSONArrayBuilder indexArrayBuilder;
        indexArrayBuilder.append (idIndex);
        collType.setIndex(indexArrayBuilder.arr());
    } else {
        BSONObjBuilder b;
        const auto featureCompatibilityVersion =
            serverGlobalParams.featureCompatibility.version.load();
        const auto indexVersion = IndexDescriptor::getDefaultIndexVersion(featureCompatibilityVersion);

        b.append("v", static_cast<int>(indexVersion));
        b.append("name", "_id_");
        b.append("ns", ns.toString());
        b.append("key", BSON("_id" << 1));
        b.append("prefix", prefix++);
        BSONArrayBuilder indexArrayBuilder;
        indexArrayBuilder.append (b.obj());
        collType.setIndex(indexArrayBuilder.arr());
    }

    ChunkType chunkType;
    //chunkType.setName();
    chunkType.setNS(ns.toString());
    chunkType.setShard(ShardId("shard0000"));
    chunkType.setMin(BSON("_id" << MINKEY));
    chunkType.setMax(BSON("_id" << MAXKEY));
    chunkType.setVersion(ChunkVersion(1, 0, epoch));
    chunkType.setStatus(ChunkType::ChunkStatus::kOffloaded);
    chunkType.setProcessIdentity("22916_1509969868746_16963381");

    std::string dbpath = storageGlobalParams.dbpath;
    char prefix_str[sizeof(prefix) + 1] = {0};
    sprintf(prefix_str, "%lld", prefix);
    dbpath = dbpath + "/" + prefix_str;

    chunkType.setRootFolder(dbpath);
    chunkType.setJumbo(false);
    chunkType.setID(prefix_str);

    index_log() << "Database::mockAssignChunk(): collectionType: ( " << collType << " ); chunkType: (" <<
        chunkType;

    BSONObjBuilder builder;
    AssignChunkRequest::appendAsCommand(&builder, chunkType, collType,  
                                       true, 
                                       "shard0000",
                                       "22916_1509969868746_16963381");

    BSONObj cmd = builder.obj();
    index_log() << "Database::mockAssignChunk(): cmd: ( " << cmd;
    auto request = AssignChunkRequest::createFromCommand(cmd);
    BSONObj cmdobj;

    Command* command = Command::findCommand("assignChunk");
    std::string errmsg;
    BSONObjBuilder result; 
    command->run(txn, "admin", cmd, 0, errmsg, result);
    return Status::OK();
}

Status userCreateNS(OperationContext* txn,
                    Database* db,
                    StringData ns,
                    BSONObj options,
                    bool createDefaultIndexes,
                    const BSONObj& idIndex) {
    invariant(db);

    index_log() << "[createCollection] userCreateNS ns: " << ns << ' ' << options << "; idIndex: " 
        << idIndex << "; createDefaultIndexes: " << createDefaultIndexes;

    if (!NamespaceString::validCollectionComponent(ns))
        return Status(ErrorCodes::InvalidNamespace, str::stream() << "invalid ns: " << ns);

    Collection* collection = db->getCollection(ns);

    if (collection)
        return Status(ErrorCodes::NamespaceExists,
                      str::stream() << "a collection '" << ns.toString() << "' already exists");

    if (db->getViewCatalog()->lookup(txn, ns))
        return Status(ErrorCodes::NamespaceExists,
                      str::stream() << "a view '" << ns.toString() << "' already exists");

    CollectionOptions collectionOptions;
    Status status = collectionOptions.parse(options);
    if (!status.isOK())
        return status;

    // Validate the collation, if there is one.
    if (!collectionOptions.collation.isEmpty()) {
        auto collator = CollatorFactoryInterface::get(txn->getServiceContext())
                            ->makeFromBSON(collectionOptions.collation);
        if (!collator.isOK()) {
            return collator.getStatus();
        }

        // If the collator factory returned a non-null collator, set the collation option to the
        // result of serializing the collator's spec back into BSON. We do this in order to fill in
        // all options that the user omitted.
        //
        // If the collator factory returned a null collator (representing the "simple" collation),
        // we simply unset the "collation" from the collection options. This ensures that
        // collections created on versions which do not support the collation feature have the same
        // format for representing the simple collation as collections created on this version.
        collectionOptions.collation =
            collator.getValue() ? collator.getValue()->getSpec().toBSON() : BSONObj();
    }

    status =
        validateStorageOptions(collectionOptions.storageEngine,
                               stdx::bind(&StorageEngine::Factory::validateCollectionStorageOptions,
                                          stdx::placeholders::_1,
                                          stdx::placeholders::_2));
    if (!status.isOK())
        return status;

    if (auto indexOptions = collectionOptions.indexOptionDefaults["storageEngine"]) {
        status =
            validateStorageOptions(indexOptions.Obj(),
                                   stdx::bind(&StorageEngine::Factory::validateIndexStorageOptions,
                                              stdx::placeholders::_1,
                                              stdx::placeholders::_2));
        if (!status.isOK()) {
            return status;
        }
    }

    if (collectionOptions.isView()) {
        uassertStatusOK(db->createView(txn, ns, collectionOptions));
    } else {
        invariant(db->createCollection(txn, ns, collectionOptions, createDefaultIndexes, idIndex));
    }

    return Status::OK();
}

Status Database::assignChunk(OperationContext* txn,
                    StringData ns,
                    BSONObj cmdObj,
                    const AssignChunkRequest& assignChunkRequest) {

    static constexpr StringData kCreateDB = "create_db"_sd;
    
    LOG(1) << "[assignChunk] DataBase::assignChunk() -0 ns: " << ns;
    invariant(txn->lockState()->isDbLockedForMode(name(), MODE_IX));
    invariant(txn->lockState()->isCollectionLockedForMode(ns, MODE_X));
    LOG(1) << "[assignChunk] DataBase::assignChunk() -1 ns: " << ns;

    Collection* collection = this->getCollection(ns,true);
    if (collection){
        //wait until assign is done
        if (collection->isAssigning()){
            
            LOG(0) << "[assignChunk] DataBase::assignChunk() -2 ns: " << ns;
            LOG (1) << "retry until chunk to be assigned completely:" << ns ;
            throw WriteConflictException();
        } 

        
        LOG(0) << "[assignChunk] DataBase::assignChunk() -3 ns: " << ns;
        return Status(ErrorCodes::NamespaceExists,
                          str::stream() << "a collection for chunk'" << ns.toString() << "' already exists");;
    }
    
    LOG(1) << "[assignChunk] DataBase::assignChunk() -4 ns: " << ns;
    //keep the code for now
    if (this->getViewCatalog()->lookup(txn, ns))
        return Status(ErrorCodes::NamespaceExists,
                      str::stream() << "a view '" << ns.toString() << "' already exists");

    // add ns to chunk map
    size_t dollar_pos= ns.find('$');
    StringData raw_ns = ns.substr(0, dollar_pos);
    StringData raw_chunk = ns.substr(dollar_pos+1, ns.size()-dollar_pos);

    ns2chunkHolder().set(raw_ns, raw_chunk.toString());

    CollectionOptions coll_Options;
    Status status = coll_Options.parse(cmdObj);
    if (!status.isOK())
    {
        error()<<"coll_Options.parse(cmdObj) is not ok, cmdObj:"<< cmdObj;
        return status;
    }
    
    coll_Options.dbPath = assignChunkRequest.getChunk().getRootFolder();
    coll_Options.toCreate = assignChunkRequest.getNewChunkFlag();
    //create rockdb instance when assign the chunk first time, prepare the chunkmetadata
    coll_Options.collection = assignChunkRequest.getCollection();
    coll_Options.chunk = assignChunkRequest.getChunk();

    LOG(1) << "Database::assignChunk()-> coll_Options 2: " << coll_Options.toBSON();
    if (!coll_Options.collation.isEmpty()) {
        auto collator = CollatorFactoryInterface::get(txn->getServiceContext())
            ->makeFromBSON(coll_Options.collation);
        if (!collator.isOK()) {
            return collator.getStatus();
        }

        // If the collator factory returned a non-null collator, set the collation option to the
        // result of serializing the collator's spec back into BSON. We do this in order to fill in
        // all options that the user omitted.
        //
        // If the collator factory returned a null collator (representing the "simple" collation),
        // we simply unset the "collation" from the collection options. This ensures that
        // collections created on versions which do not support the collation feature have the same
        // format for representing the simple collation as collections created on this version.
        coll_Options.collation =
            collator.getValue() ? collator.getValue()->getSpec().toBSON() : BSONObj();
    }
    LOG(1) << "Database::assignChunk()-> coll_Options.collation: " << coll_Options.collation;
    status =
        validateStorageOptions(coll_Options.storageEngine,
                              stdx::bind(&StorageEngine::Factory::validateCollectionStorageOptions,
                                         stdx::placeholders::_1,
                                         stdx::placeholders::_2));
    if (!status.isOK())
        return status;

    if (auto indexOptions = coll_Options.indexOptionDefaults["storageEngine"]) {
        status =
            validateStorageOptions(indexOptions.Obj(),
                                  stdx::bind(&StorageEngine::Factory::validateIndexStorageOptions,
                                             stdx::placeholders::_1,
                                             stdx::placeholders::_2));
        if (!status.isOK()) {
            return status;
        }
    }

    if (coll_Options.isView()) {
        LOG(1) << "Database::assignChunk()-> createView";
        uassertStatusOK(createView(txn, ns, coll_Options));
        return Status::OK();
    } 




    NamespaceString nss(ns);
    _checkCanCreateCollection(nss, coll_Options);
    audit::logCreateCollection(&cc(), ns);

    //KVDatabaseCatalogEntry to create record in mdb_catalog
    getGlobalServiceContext()->getProcessStageTime(
        "assignChunk:"+assignChunkRequest.getChunk().getName())->noteStageStart("createCollection");
    status =  _dbEntry->createCollection(txn, ns, coll_Options, true);
    massertNoTraceStatusOK(status);

    LOG(1) << "[assignChunk] DataBase::assignChunk() -5 finish createCollection, ns: " << ns;   
    txn->recoveryUnit()->registerChange(new AddCollectionChange(txn, this, ns));
    collection = _getOrCreateCollectionInstance(txn, ns);

    LOG(1) << "[assignChunk] DataBase::assignChunk() -6 finish _getOrCreateCollectionInstance, ns: " << ns;   

    CollectionType coll_type = assignChunkRequest.getCollection();
    collection->setCollTabType(coll_type.getTabType());
    invariant(collection);
    collection->setAssigning(true);

    // add  the collection_catalog_entry
    {
        stdx::lock_guard<stdx::mutex> lk(_collectionsMutex);
        _collections[ns] = collection;
    }

    // for the first assignment of chunk, config server will bring metadata
    BSONObj fullIdIndexSpec; 
    auto opObserver = getGlobalServiceContext()->getOpObserver();
    if (opObserver)
        opObserver->onCreateCollection(txn, nss, coll_Options, fullIdIndexSpec);

    LOG(1) << "[assignChunk] DataBase::assignChunk() -7 end ns: " << ns;
    
    return Status::OK();
}

void Database::assignChunkFinalize(OperationContext* txn,
                    StringData ns, const AssignChunkRequest& assignChunkRequest){
    Collection* collection = this->getCollection(ns, true);
    invariant(collection);
    if (!GLOBAL_CONFIG_GET(IsCoreTest)) {
        invariant(!txn->lockState()->isDbLockedForMode(name(), MODE_IX));
        invariant(!txn->lockState()->isCollectionLockedForMode(ns, MODE_X));
    }
    
    try{
        CollectionOptions options;
        options.dbPath = assignChunkRequest.getChunk().getRootFolder();
        options.toCreate = assignChunkRequest.getNewChunkFlag();
        
        //create rockdb instance when assign the chunk first time, prepare the chunkmetadata
        options.collection = assignChunkRequest.getCollection();
        options.chunk = assignChunkRequest.getChunk();
        _dbEntry->postInitRecordStore(txn,ns,options);
        // get current index specs
        NamespaceString nss(ns);
        BSONArrayBuilder indexArrayBuilder;
        BSONObjIterator indexspecs(options.collection.getIndex());
        while (indexspecs.more()) {
            indexArrayBuilder.append(indexspecs.next());
        }

        BSONObjBuilder builder;
        builder.appendArray("indexes", indexArrayBuilder.arr());
        BSONObj indexes = builder.obj();
        auto specsWithStatus =
            parseAndValidateIndexSpecs(nss, indexes, serverGlobalParams.featureCompatibility);

        if (!specsWithStatus.isOK()) {
            error()<<"Database::assignChunkFinalize specsWithStatus is not ok";
            return;
        }

        auto specs = std::move(specsWithStatus.getValue());
        auto indexSpecsWithDefaults =
            resolveCollectionDefaultProperties(txn, collection, std::move(specs));
        if (!indexSpecsWithDefaults.isOK()) {
            error()<<"Database::assignChunkFinalize indexSpecsWithDefaults is not ok";
            return;
        }

        specs = std::move(indexSpecsWithDefaults.getValue());

        bool createIdIndex = collection->requiresIdIndex() && 
            (options.autoIndexId == CollectionOptions::YES ||
            options.autoIndexId == CollectionOptions::DEFAULT); 

        //bool createIdIndex = true;
        //if (options.collection.getOptions().hasField("autoIndexId")) {
        //    if (options.collection.getOptions().getBoolField("autoIndexId")) {
        //        index_log() << "[assignChunk] Database::assignChunkFinalize() autoIndexId: true";
        //        createIdIndex = true;
        //    } else {
        //        index_log() << "[assignChunk] Database::assignChunkFinalize() autoIndexId: false";
        //        createIdIndex = false;
        //    }
        //}

        if (!createIdIndex) {
            for(auto it=specs.begin(); it!=specs.end(); ++it) {
                BSONObj key = it->getField("key").Obj();
                if (key.hasField("_id")) {
                    specs.erase(it);
                    break;
                }
            }
        } else {
            bool existIdIndex = false;
            for(auto it=specs.begin(); it!=specs.end(); ++it) {
                BSONObj key = it->getField("key").Obj();
                if (key.hasField("_id")){
                    existIdIndex = true;
                    break;
                } 
            }
            
            if (nss.isSystem()) {
                authindex::createSystemIndexes(txn, collection);
            }
        }

        if (specs.empty()) {
            warning()<<"Database::assignChunkFinalize specs is empty";
            collection->setAssigning(false);
            return;
        }
       
        ScopedTransaction transaction(txn, MODE_IX);
        Lock::DBLock dbLock(txn->lockState(), nss.db(), MODE_IX);
        Lock::CollectionLock collLock(txn->lockState(), nss.ns(), MODE_X);
        MultiIndexBlock indexer(txn, collection);
        std::vector<BSONObj> indexInfoObjs;
        //indexer will add index metadata into mdb_catalog
        indexInfoObjs = uassertStatusOK(indexer.init(specs));

        WriteUnitOfWork wunit(txn);
        indexer.commit();
        for (auto&& infoObj : indexInfoObjs) {
            std::string systemIndexes = nss.getSystemIndexesCollection();
            auto opObserver = getGlobalServiceContext()->getOpObserver();
            if (opObserver) {
                opObserver->onCreateIndex(txn, systemIndexes, infoObj);
            }
        }
        wunit.commit();
          
    }
    catch(const std::exception &exc){
        // catch anything thrown within try block that derives from std::exception
        log() << exc.what();
        log() << "assignChunkFinalize failed";
        
        // TODO: not to assert if assign failed
        invariant (false); 
    }

    collection->setAssigning(false);
    //log()<<"assignChunkFinalize "<<reinterpret_cast<unsigned long long>(this)<<" "<<ns;
    return;
}

Status Database::toUpdateChunkMetadata(OperationContext* txn,StringData ns,BSONArray &indexes){
    return _dbEntry->updateChunkMetadataViaRecordStore(txn,ns,indexes);
}

}  // namespace mongo
