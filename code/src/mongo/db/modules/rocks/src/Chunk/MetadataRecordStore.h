#pragma once

#include <boost/shared_array.hpp>
#include <map>

#include "mongo/db/storage/capped_callback.h"
#include "mongo/db/storage/ephemeral_for_test/ephemeral_for_test_record_store.h"
#include "mongo/db/storage/record_store.h"

namespace mongo {

    //  This class maintains metadata in memory and should be a singleton.
    //  Currently is based on EphemeralForTestRecordStore.
    //  TODO: Need to rewrite it to some more reliable ephemeral storage impplementation
    class MetadataRecordStore : public mongo::EphemeralForTestRecordStore {
    protected:
        std::shared_ptr<void> dataInOut;

    public:
        explicit MetadataRecordStore()
            : mongo::EphemeralForTestRecordStore("_mdb_catalog",  //  StringData ns
                                                 &dataInOut,  //  std::shared_ptr<void>* dataInOut
                                                 false,       //  bool isCapped
                                                 0,           //  int64_t cappedMaxSize
                                                 0,           //  int64_t cappedMaxDocs
                                                 nullptr      //  CappedCallback* cappedCallback
                                                 ) {}

        virtual const char* name() const override { return "MetadataRecordStore"; }
    };

}  // namespace mongo
