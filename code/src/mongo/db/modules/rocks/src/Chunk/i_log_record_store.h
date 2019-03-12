#pragma once

#include <rocksdb/slice.h>
#include <rocksdb/status.h>
#include <rocksdb/shared_resource.h>
#include "ChunkMetadata.h"


namespace mongo
{

namespace TransactionLog
{

    typedef uint32_t uint32;
enum LogRecordType
{
    TransactionLogInternal,
    LoadBalancingTransactionLog,
    SharedResourceFilterLog,
    Metadata
};

//
//  Order in which ILogRecordStore is going to replay records.
//  If provider doesn't support order it can let ILogRecordStore knows
//  by return NotSupported as a status.
//
enum LogReplayOrder
{
    Arbitrary = 0,  //  No particular order
    Forward,        //  Records will be replayed in the order in which they were written
    Reverse         //  Records will be replayed in reverse order to what they were written
};

///////////////////////////////////////////////////////////////////////////////////////////////////

#define INVALID_FILE_SN  (std::numeric_limits<uint64_t>::max())
#define INVALID_LSN  (std::numeric_limits<uint64_t>::max())

class LogRecord
{
protected:
    const LogRecordType type;
    uint64_t lsn;  // setted by ILogRecordStore
    const rocksdb::Slice slice;

public:
    LogRecord(LogRecordType type, const rocksdb::Slice& slice) : type(type), slice(slice)
    {
        invariant(slice.data() != nullptr);
        invariant(slice.size() != 0);
        lsn = INVALID_LSN;

    }
    LogRecord(LogRecordType type, const char* data, size_t size) : LogRecord(type, rocksdb::Slice(data, size))
    {
    }

    const LogRecordType GetType() const
    {
        return type;
    }

    const rocksdb::Slice& GetSlice() const
    {
        return slice;
    }

    const char* GetData() const
    {
        return slice.data();
    }

    const size_t GetSize() const
    {
        return slice.size();
    }

    void SetLsn(uint64_t s)
    {
        lsn = s;
    }

    const uint64_t GetLsn() const
    {
        return lsn;
    }
    // only used by UT
    bool operator==(const LogRecord &a)
    {
        return type == a.type && slice.size()==a.slice.size();
    }
};


//
//  Interface which reprensents write functionality of Log Record Store.
//  The instance of this interface will be provided to Log Record Provider during initialization.
//
class ILogRecordWriter
{
public:
    // write mutilpue records in one write
    virtual rocksdb::Status WriteRecord(std::vector<LogRecord>& records) = 0;

    // write one record in one write
    virtual rocksdb::Status WriteRecord(LogRecord& record) = 0;


    //
    //  Destructor
    //
    virtual ~ILogRecordWriter() { }
};


///////////////////////////////////////////////////////////////////////////////////////////////////
//
//  Interface represent particular record provider, which needs to save records in log structured way.
//
//  The life-cycle of the provider is as follows:
//      1. Provider needs to register itself with ILogRecordStore::RegisterProvier().
//      1.1. During the initialization, ILogRecordStore calls OnInitializationStart().
//      1.2. ILogRecordStore provides the order in which it's gonig to replay logs. If provider doesn't
//          support this order it can return NotSupported. In this case ILogRecordProvider can try
//          to propose different order.
//      1.3. ILogRecordStore associate provider with the list of record types, that this provider
//          supports. It gets them from supportedRecordTypes argument of InitializationBegin.
//      2. After opening of record source (e.g. Chunk loading) ILogRecordStore calls ReplayLogRecord for
//          each record it has saved after the start of latest successfull checkpoint. Note: records
//          can be replayed in arbitrary order. If order matters for provider it needs to maintain LSN and
//          resort records. During the replay, provider can let ILogRecordStore know that it doesn't need
//          more records.
//      2.1. When all records are replayed, ILogRecordStore is ready to accept write requests from provider
//          and call InitializationEnd provider "writer" object. Providers need to save this objects to write
//          records and checkpoint.
//      3. When ILogRecordStore observes that there are too many log records, it tries to reduce amount of
//          records by writing checkpoint. For that it firstly calls CheckpointBegin for each provider.
//      3.1. Then ILogRecordStore calls WriteCheckpoint for each provider.
//      3.2. When all providers have written their checkpoints ILogRecordStore calls CheckpointEnd.
//      4. When log source is closing (e.g. chunk offload) ILogRecordStore calls UnloadProvider. It's
//          the last opportunity for provider to save something in the store.
//
class ILogRecordProvider
{
public:
    //
    //  Called for each provider before log replay. Provider needs to return list of supported record types
    //  in 'supportedRecordTypes'. If provider doesn't support replay order it can return NotSupported.
    //
    virtual rocksdb::Status InitializationBegin(
        LogReplayOrder replayOrder,
        std::vector<LogRecordType>& supportedRecordTypes
        ) = 0;

    //
    //  Called by ILogRecordStore for each record related to current provider and saved in store after the
    //  start of latest successfull checkpoint. This function will be called only between InitializationBegin and
    //  InitializationEnd calls. If provider has enough data it can set needMoreRecords to false and store won't
    //  call provider after that.
    //
    virtual rocksdb::Status ReplayLogRecord(const LogRecord& record, bool& needMoreRecords) = 0;

    //
    //  Called when all records related to this provider were read and processed.
    //  After this moment provider can start writing to ILogRecordStore using "writer"
    //
    virtual rocksdb::Status InitializationEnd(ILogRecordWriter& writer) = 0;

    //
    //  Called when IRecordStore decided to checkpoint current state of providers.
    //  Poviders can use this function to block writes to IRecordStore before checkpoint is finished.
    //  This function can be ommited if provider are able to aggregate records saved during checkpointing.
    //  In this case provider don't need to block writes, since it can merge them with checkpoint.
    //
    virtual void CheckpointBegin() = 0;

    //
    //  This function is called between CheckpointBegin and CheckpointEnd
    //  In this function provider should save it's' state: aggregated representation of all
    //  valid records. if checkpoint succed for all providers, records written before CheckpointBegin
    //  will be discarded and never be replayed through ReplayLogRecord.
    //
    virtual rocksdb::Status WriteCheckpoint() = 0;

    //
    //  Called after all providers saved their checkpoints.
    //
    virtual void CheckpointEnd() = 0;

    //
    //  Called before record store is closed. After that, it's not allowed to use writer.
    //
    virtual void UnloadProvider() { }

    //
    // get provider name
    //
    virtual std::string GetName() = 0;

    //
    // rewrite the write failed logs, which must be written before you write new logs
    // This function is called by ILogRecordStore
    virtual rocksdb::Status RewriteImportantFailedLog() = 0;

    //
    //  Destructor
    //
    virtual ~ILogRecordProvider() { }
};  //  ILogRecordProvider
///////////////////////////////////////////////////////////////////////////////////////////////////


///////////////////////////////////////////////////////////////////////////////////////////////////
//
//  Interface that represent log record provider
//
class ILogRecordStore
{
public:
    //
    //  Register provider in store and call providers InitializationBegin
    //
    virtual rocksdb::Status RegisterProvider(ILogRecordProvider& provider) = 0;

    //
    //  Destructor
    //
    virtual ~ILogRecordStore() { }
};
///////////////////////////////////////////////////////////////////////////////////////////////////


}   //  namespace TransactionLog

}   //  namespace mongo

