
#pragma once

#include <mongo/stdx/memory.h>
#include <rocksdb/env.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <atomic>
#include <mutex>
#include <string>
#include <vector>
#include "../mongo_rocks_result_handling.h"
#include "i_log_record_store.h"
//#include "i_shared_resource_manager.h"
//#include "IndexedRecordStorage.h"

namespace mongo {

    namespace TransactionLog {

#define MAX_FILE_OFFSET (std::numeric_limits<uint32_t>::max())

///////////////////////////////////////////////////////////////////////////////////////////////////
#define TRANS_LOG_HEAD_TLV_MAGIC 0XACEACE88

        // this struct is the storage format in disk. if modify, must consider how to upgrade
        struct TransLogHeadTlv {
            uint32_t magic;    // set as TRANS_LOG_HEAD_TLV_MAGIC
            uint32_t crc32;    // tlv crc; // The start position of crc computing
            uint16_t version;  // trans log format version, now is 0
            uint16_t pad_len;
            uint32_t total_len;  // total lenth of record , include TransLogHeadTlv
            uint64_t reserve64[2];
            uint8_t data[0];
        };

        // this struct is the storage format in disk. if modify, must consider how to upgrade
        struct TransLogSubTlv {
            uint8_t record_type;  // set as LogRecordType
            uint8_t reserve8;
            uint16_t len;  // the lenght of recode include TransLogSubTlv
            uint32_t reserve32;
            uint64_t lsn;  // log number
            uint8_t data[0];
        };

#define TransLogHeadTlvEncodeLen sizeof(TransLogHeadTlv)

#define TransLogSubTlvEncodeLen sizeof(TransLogSubTlv)

        enum class SystemTlogOperationType : uint8_t {
            none = 0,
            StartCheckpoint = 1,
            CommitCheckpoint = 2,
            AbortCheckpoint = 3
        };

        // this struct is the storage format in disk for checkpoint
        struct CheckpointDiskFormat {
            uint64_t checkpoint_start_lsn;  // if checkpoint start, lsn is setted to lsn of
                                            // checkpoint start
            // if checkpoint commit, lsn is setted to lsn of checkpoint start
            uint8_t operation_type;  // set as SystemTlogOperationType
            uint8_t reserve8[3];
            uint32_t reserve32;
        };
#define CheckpointEncodeLen sizeof(CheckpointDiskFormat)

        ///////////////////////////////////////////////////////////////////////////////////////////////////

        struct TransLogFileMetaData {
            uint64_t file_sn;
            uint64_t file_size;
            uint64_t largest_lsn;
        };

        // Use env to write data to a file.
        class TransLogFileWriter {
        private:
            std::unique_ptr<rocksdb::WritableFile> writable_file_;
            // Actually written data size can be used for truncate
            // not counting padding data
            uint64_t file_size_;

            uint64_t file_sn_;

            uint64_t largest_lsn_;

        public:
            TransLogFileWriter(std::unique_ptr<rocksdb::WritableFile>&& file, uint64_t file_sn)
                : writable_file_(std::move(file)),
                  file_size_(0),
                  file_sn_(file_sn),
                  largest_lsn_(0) {}

            TransLogFileWriter(const TransLogFileWriter&) = delete;

            TransLogFileWriter& operator=(const TransLogFileWriter&) = delete;

            ~TransLogFileWriter() { Close(); }

            rocksdb::Status Append(const rocksdb::Slice& data, const uint64_t largest_lsn);

            rocksdb::Status Flush() { return writable_file_->Flush(); }

            rocksdb::Status Close();

            rocksdb::Status Sync(bool use_fsync) { return writable_file_->Sync(); }

            uint64_t GetFileSize() { return file_size_; }

            uint64_t GetFileSn() { return file_sn_; }

            uint64_t GetLargestLsn() { return largest_lsn_; }

            rocksdb::WritableFile* writable_file() const { return writable_file_.get(); }
        };

        class TranslogMutex {
        public:
            TranslogMutex() { locked_ = false; }
            ~TranslogMutex() {}

            void Lock() {
                mu_.lock();
                locked_ = true;
            };
            void Unlock() {
                mu_.unlock();
                locked_ = false;
            }
            // this will assert if the mutex is not locked
            // it does NOT verify that mutex is held by a calling thread
            void AssertHeld() { assert(locked_); }

        private:
            std::mutex mu_;
            bool locked_;
            // No copying
            TranslogMutex(const TranslogMutex&);
            void operator=(const TranslogMutex&);
        };

        class TranslogMutexLock {
        public:
            explicit TranslogMutexLock(TranslogMutex* mutex) : mutex_(mutex) { mutex_->Lock(); }

            ~TranslogMutexLock() { mutex_->Unlock(); }

        private:
            TranslogMutex* const mutex_;
            TranslogMutexLock(const TranslogMutexLock&) = delete;
            void operator=(const TranslogMutexLock&) = delete;
        };

        // provider per record type
        struct RecordTypeProvider {
            bool isNeedMoreRecords;
            LogRecordType type;
            ILogRecordProvider* provider;
        };

        // location info for checkpont
        struct CheckpointLocation {
            uint64_t checkpoint_start_file_sn;
            uint64_t self_file_sn;
            // in the case of checkpoint start, checkpoint_start_lsn is zero
            // in the cases of checkpoint commit and about, checkpoint_start_lsn is
            // the lsn of the checkpoint start
            uint64_t checkpoint_start_lsn;
            // the lsn in self TransLogHeadTlv
            uint64_t self_lsn;
            // in the case of invalid checkpoint and commit, checkpoint_start_offset is offset of
            // checkpoint start
            // in the cases of checkpoint start ,checkpoint_start_offset is equal to self_offset
            uint32_t checkpoint_start_offset;
            uint32_t self_offset;
        };

#define IsCheckpointLocationValid(checkpoint) ((checkpoint).self_file_sn != INVALID_FILE_SN)

        struct ValidStartEndPosition {
            uint64_t file_sn;
            uint32_t start_offset;
            uint32_t end_offset;
        };

        class TransLogRecordWriter : public ILogRecordWriter {
        public:
            TransLogRecordWriter() { trans_log_Record_store_ = nullptr; }

            ~TransLogRecordWriter() {}

            rocksdb::Status Init(ILogRecordStore* trans_log_Record_store) {
                trans_log_Record_store_ = trans_log_Record_store;
                return rocksdb::Status::OK();
            }

            // write mutilpue records in one write
            rocksdb::Status WriteRecord(std::vector<LogRecord>& record, bool allow_check_point);

            // write one record in one write
            rocksdb::Status WriteRecord(LogRecord& record, bool allow_check_point);

        private:
            ILogRecordStore* trans_log_Record_store_;
        };

        //  ILogRecordStore for trans log
        class TransLogRecordStore : public ILogRecordStore {
        public:
            TransLogRecordStore(rocksdb::Env* env, const std::string& db_path) {
                next_file_sn_ = 1;
                lsn_ = 1;
                env_ = env;
                trans_log_path_ = db_path + "/translog";

                checkpoint_commit_.checkpoint_start_file_sn = INVALID_FILE_SN;
                checkpoint_commit_.self_file_sn = INVALID_FILE_SN;
                checkpoint_commit_.checkpoint_start_lsn = INVALID_LSN;
                checkpoint_commit_.self_lsn = INVALID_LSN;
                checkpoint_commit_.checkpoint_start_offset = 0;
                checkpoint_commit_.self_offset = 0;
                invalid_checkpoint_list_.clear();

                static_trans_log_current_size_ = 0;
                writing_checkpoint_ = false;
                failed_checkpoint_start_lsn_ = INVALID_LSN;

                writable_files_.clear();
                alive_log_files_.clear();
                registered_provider_.clear();
                duplicate_lsn_ = 0;
                duplicate_crc_ = 0;
            }

            rocksdb::Status Init();

            //  Register provider in store and call providers InitializationBegin
            rocksdb::Status RegisterProvider(ILogRecordProvider& provider);

            // write multiple record in one write
            rocksdb::Status WriteRecord(std::vector<LogRecord>& record, bool allow_check_point);

            // write onew record in one write
            rocksdb::Status WriteRecord(LogRecord& record, bool allow_check_point);

            rocksdb::Status CreateNewWriteFileNoLock(TransLogFileWriter** new_writer);

            TransLogFileWriter* GetCurrentLogFileWriter(void);

            rocksdb::Status CloseWriteFile(TransLogFileWriter* old_file_writer);

            void DeleteSmallerWriteFile(uint64_t file_sn);

            uint32_t GetRecordEncodeLen(const std::vector<LogRecord>& record);

            rocksdb::Status WriteOneTlv(char* buf, uint32_t valid_size);

            rocksdb::Status WriteRecordInternal(std::vector<LogRecord>& record);

            // write trans log for checkpoint
            rocksdb::Status LogCheckPointState(const SystemTlogOperationType type,
                                               const uint64_t checkpoint_start_lsn,
                                               uint64_t& new_log_lsn);

            void GetProvider8RecordType(LogReplayOrder replayOrder,
                                        std::vector<RecordTypeProvider>* record_type_provider_list);

            rocksdb::Status WriteImportantFailLog4CheckpointAbout(void);

            // decode checkpoint for get checkpoint
            bool DecodeCheckpoint(const uint64_t file_sn, const uint64_t offset_in_file,
                                  const TransLogSubTlv* sub_tlv, bool& get_checkpoint);

            bool InvertGetCheckpoint(const uint64_t file_sn, const uint64_t offset_in_file,
                                     const rocksdb::Slice& data, bool& get_checkpoint,
                                     uint32_t& decode_len);

            rocksdb::Status InvertReadTransLogFils(const uint64_t file_sn, char* buf,
                                                   const uint32_t buf_len, bool& get_checkpoint);

            rocksdb::Status GetCheckpoint4Replay(bool& need_replay);

            bool GetTransLogReplayStartAndEndPos(
                std::vector<ValidStartEndPosition>* valid_position_list);
            // Forward replay trans log
            rocksdb::Status ForwardReplayTransLog(
                std::vector<RecordTypeProvider>& record_type_provider, uint64_t& max_replayed_lsn);

            rocksdb::Status ForwardReadOneTransLogFile(
                std::vector<RecordTypeProvider>& record_type_provider, const uint64_t file_sn,
                uint32_t start_offset, const uint32_t end_offset, const uint64_t file_size,
                std::string file_name, uint64_t& max_replayed_lsn);
            rocksdb::Status CloseAllWriteFile();

            rocksdb::Status WriteCheckPoint(bool realloc_lsn);
            rocksdb::Status CheckAndWriteCheckPoint(void);

            //
            //  Destructor
            virtual ~TransLogRecordStore() {}

            uint64_t GetLsn() { return lsn_.load(std::memory_order_relaxed); }
            void SetLsn(uint64_t x) { lsn_.store(x, std::memory_order_relaxed); }

        private:
            // db path which store trans log files
            std::string trans_log_path_;

            std::atomic<uint64_t> next_file_sn_;

            std::atomic<uint64_t> lsn_;

            // TransLogRecordWriter
            TransLogRecordWriter record_writer_;

            // trans log write to plog after align to 128
            static const uint32_t pad_size_ = 128;

            // data in Append() is no longer than 1MB
            static const uint32_t max_buf_size_ = 1048576;

            // if the total size of trans log is larger than max_total_trans_log_size_,
            // we will write a checkpoint
            const uint32_t max_total_trans_log_size_ = 1048576;  // 8MB

            // tatal size of the alive trans logs
            std::atomic<uint64_t> static_trans_log_current_size_;

            // tatal size of the alive trans logs at the latest check point
            std::atomic<uint64_t> static_trans_log_old_size_;

            // if true, one thread is writ
            std::atomic<bool> writing_checkpoint_;

            // Log files that do not complete data write, and the current log file.
            // log files if created, first time added to writable_files_
            // after complete data write, log files move to  alive_log_files_
            // after checkpoint, log files move to files_to_free_

            // protect alive_log_files_ and files_to_free_ and writable_files_
            std::mutex log_file_lock_;

            std::vector<TransLogFileWriter*> writable_files_;

            // alive log files, olny for read
            std::vector<TransLogFileMetaData> alive_log_files_;

            // If this is non-empty, we need to delete these log files in background
            // threads. Protected by db mutex.
            // std::vector<TransLogFileMetaData> files_to_free_;

            rocksdb::Env* env_;

            // maintain registered provider
            std::vector<ILogRecordProvider*> registered_provider_;

            // maintain checkpoint
            // location info for checkpont start
            // CheckpointLocation   checkpoint_start_;
            // location info for checkpont commit
            CheckpointLocation checkpoint_commit_;
            // invalid checkpont list
            std::vector<CheckpointLocation> invalid_checkpoint_list_;

            // the lsn of checkpoint_start which failed to write checkpoint_about
            // Before we write any new logs, we must write checkpoint_about success first
            uint64_t failed_checkpoint_start_lsn_;
            // check duplicate log record
            uint64_t duplicate_lsn_;
            uint32_t duplicate_crc_;

            // protect write operation of each thread
            TranslogMutex write_lock_;

        private:
            std::string MakeFileName(uint64_t file_sn) {
                char buf[64];
                snprintf(buf,  sizeof(buf) - 1, "/%llu.tlog",
                           static_cast<unsigned long long>(file_sn));
                return trans_log_path_.c_str() + std::string(buf);
            }

            bool ParseFileName(const std::string& file_path, uint64_t* file_sn);

            bool ParseFileName(const char* file_name, uint64_t* file_sn);

            uint64_t IncreaseLsn() { return lsn_.fetch_add(1, std::memory_order_relaxed); }

            ILogRecordProvider* GetRecordProvider(
                const LogRecordType type,
                std::vector<RecordTypeProvider>& record_type_provider_list,
                bool& isNeedMoreRecords);
            // Save Record Porviders flag according to record_type
            void SaveRecordProviderFlag(const LogRecordType type,
                                        std::vector<RecordTypeProvider>& record_type_provider_list,
                                        bool& needMoreRecords);

            rocksdb::Status DecodeOneHeadTlv(std::vector<RecordTypeProvider>& record_type_provider,
                                             const TransLogHeadTlv* head_tlv,
                                             uint32_t& already_decode_len,
                                             uint64_t& max_replayed_lsn);

            rocksdb::Status DecodeOneSlice(const rocksdb::Slice& data,
                                           std::vector<RecordTypeProvider>& record_type_provider,
                                           uint32_t& decode_len, uint64_t& max_replayed_lsn);

            uint64_t GetMinFileSn();

            void ClearTransLogFiles(void);
        };

    }  //  namespace TransactionLog

}  //  namespace mongo
