
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "TransLogRecordStore.h"
#include "../mongo_rocks_result_handling.h"
#include "../rocks_engine.h"
#include "base_crc.h"
#include "mongo/platform/basic.h"
#include "mongo/util/fail_point_service.h"
#include "mongo/util/log.h"

namespace mongo {

    // Failpoint
    MONGO_FP_DECLARE(failToCheckAndWriteCheckPoint);
    MONGO_FP_DECLARE(failToWriteRecordInternal);

    namespace TransactionLog {

        bool CompareTransLogFileSn(const struct TransLogFileMetaData& first_file,
                                   const struct TransLogFileMetaData& second_file) {
            return (first_file.file_sn < second_file.file_sn);
        }

        // metadata online check : tlv crc
        // The start position of crc computing
        static const uint64_t TransLogHeadTlvCrcStart = IndexOffsetOf(TransLogHeadTlv, version);

        void AddTlvCRC32(TransLogHeadTlv* p_tlv) {
            uint32_t crc = INDEX_CRC_INIT_VALUE;
            crc = IndexCRC32(crc, (char*)((char*)p_tlv + TransLogHeadTlvCrcStart),
                             p_tlv->total_len - TransLogHeadTlvCrcStart);
            p_tlv->crc32 = crc;
            return;
        }

        bool CheckTlvCrc(const TransLogHeadTlv* p_tlv) {
            uint32_t crc = INDEX_CRC_INIT_VALUE;
            crc = IndexCRC32(crc, (char*)((char*)p_tlv + TransLogHeadTlvCrcStart),
                             p_tlv->total_len - TransLogHeadTlvCrcStart);
            if (p_tlv->crc32 != crc) {
                index_err() << " p_tlv->crc32:" << p_tlv->crc32 << " crc:" << crc
                            << " tlv len:" << p_tlv->total_len;
            }
            return p_tlv->crc32 == crc ? true : false;
        }

        // write mutilpue records in one write
        rocksdb::Status TransLogRecordWriter::WriteRecord(std::vector<LogRecord>& record,
                                                          bool allow_check_point) {
            TransLogRecordStore* store = (TransLogRecordStore*)(trans_log_Record_store_);
            return store->WriteRecord(record, allow_check_point);
        }

        // write one record in one write
        rocksdb::Status TransLogRecordWriter::WriteRecord(LogRecord& record,
                                                          bool allow_check_point) {
            TransLogRecordStore* store = (TransLogRecordStore*)(trans_log_Record_store_);
            return store->WriteRecord(record, allow_check_point);
        }

        ///////////////////////////////maintain trans log file ,include:
        /// create\append///////////////////////////////////

        rocksdb::Status TransLogFileWriter::Append(const rocksdb::Slice& data,
                                                   const uint64_t largest_lsn) {
            rocksdb::Status s = writable_file_->Append(data);
            if (s.ok()) {
                s = writable_file_->Fsync();
            }
            if (s.ok()) {
                file_size_ += data.size();
                largest_lsn_ = largest_lsn;
            }
            return s;
        }

        rocksdb::Status TransLogFileWriter::Close() {
            // Do not quit immediately on failure the file MUST be closed
            rocksdb::Status s;

            // Possible to close it twice now as we MUST close
            // in __dtor, simply flushing is not enough
            // Windows when pre-allocating does not fill with zeros
            // also with unbuffered access we also set the end of data.
            if (!writable_file_) {
                return s;
            }

            s = Flush();  // flush cache to OS
            rocksdb::Status interim = writable_file_->Close();
            if (!interim.ok() && s.ok()) {
                s = interim;
            }
            writable_file_.reset();

            return s;
        }

        bool TransLogRecordStore::ParseFileName(const std::string& file_path, uint64_t* file_sn) {
            char* ptr = NULL;
            index_log() << "TransLogRecordStore::ParseFileName() " << file_path;
            char fname[128] = {0};

            if (file_path.size() >= 128) {
                index_err() << "too long, file_path:" << file_path;
                return false;
            }
            ptr = strrchr(const_cast<char*>(file_path.c_str()), '/');
            if ((nullptr == ptr)) {
                strncpy(fname, file_path.c_str(), file_path.size());
            } else {
                ptr++;  // skip '/'
                strncpy(fname,  ptr, strlen(ptr));
            }

            index_log() << "TransLogRecordStore::ParseFileName() 1 fname: " << fname;

            if (0 == strlen((char*)fname)) {
                return false;
            }

            if ((ptr = strstr((char*)fname, ".tlog")) != nullptr) {
                *file_sn = strtoull(fname, &ptr, 10);
                return true;
            }
            return false;
        }

        bool TransLogRecordStore::ParseFileName(const char* file_name, uint64_t* file_sn) {
            char* ptr = nullptr;
            if (0 == strlen((char*)file_name)) {
                return false;
            }

            if ((ptr = strstr((char*)file_name, ".tlog")) != nullptr) {
                *file_sn = strtoull(file_name, &ptr, 10);
                return true;
            }
            return false;
        }

        rocksdb::Status TransLogRecordStore::CreateNewWriteFileNoLock(
            TransLogFileWriter** new_writer) {
            std::unique_ptr<rocksdb::WritableFile> writable_file;
            rocksdb::Status s;
            std::string file_name = MakeFileName(next_file_sn_);

            *new_writer = nullptr;
            RES_RIF(env_->NewWritableFile(file_name, &writable_file, rocksdb::EnvOptions()));

            TransLogFileWriter* file_writer =
                new TransLogFileWriter(std::move(writable_file), next_file_sn_);
            if (nullptr == file_writer) {
                s = rocksdb::Status::NoSpace("new file_writer fail");
            }
            RES_RIF(s);
            // add  file_writer to writable_files_
            writable_files_.push_back(file_writer);
            next_file_sn_++;
            *new_writer = file_writer;
            return rocksdb::Status::OK();
        }

        TransLogFileWriter* TransLogRecordStore::GetCurrentLogFileWriter(void) {
            std::lock_guard<std::mutex> lock(log_file_lock_);

            if (0 == writable_files_.size()) {
                TransLogFileWriter* writer = NULL;
                // if not have any writer, create a new writer
                rocksdb::Status s = CreateNewWriteFileNoLock(&writer);
                if (!s.ok()) {
                    index_err() << "get new trans log file writer:" << s.ToString();
                    return nullptr;
                }
                return writer;
            } else {
                // get the first writer
                return writable_files_[0];
            }
        }

        rocksdb::Status TransLogRecordStore::CloseWriteFile(TransLogFileWriter* old_file_writer) {
            TransLogFileMetaData file_metadata;

            file_metadata.file_size = old_file_writer->GetFileSize();
            file_metadata.file_sn = old_file_writer->GetFileSn();
            file_metadata.largest_lsn = old_file_writer->GetLargestLsn();

            std::lock_guard<std::mutex> lock(log_file_lock_);
            // add  file to alive_log_files_
            alive_log_files_.push_back(file_metadata);

            // remove file from writable_files_
            for (size_t i = 0; i < writable_files_.size(); i++) {
                if (writable_files_[i]->GetFileSn() == old_file_writer->GetFileSn()) {
                    std::vector<TransLogFileWriter*>::iterator it = writable_files_.begin() + i;
                    writable_files_.erase(it);
                    delete old_file_writer;
                    old_file_writer = nullptr;
                    break;
                }
            }

            if (nullptr != old_file_writer) {
                index_err() << "file:" << old_file_writer->GetFileSn() << " not in writable_files_";
            }
            return rocksdb::Status::OK();
        }

        // delete files which file_sn is smaller than  file_sn
        void TransLogRecordStore::DeleteSmallerWriteFile(uint64_t file_sn) {
            return;
            std::string file_name;
            rocksdb::Status s;

            std::lock_guard<std::mutex> lock(log_file_lock_);

            // delete old log files
            for (size_t i = 0; i < alive_log_files_.size(); i++) {
                file_name.clear();
                if (alive_log_files_[i].file_sn < file_sn) {
                    file_name = MakeFileName(file_sn);
                    s = deleteFile(file_name);
                    index_err() << "DeleteFile:" << file_name << " return:" << s.ToString();
                    if (!s.ok()) {
                        continue;
                    }
                    std::vector<TransLogFileMetaData>::iterator it = alive_log_files_.begin() + i;
                    alive_log_files_.erase(it);
                }
            }
        }

        uint32_t TransLogRecordStore::GetRecordEncodeLen(const std::vector<LogRecord>& record) {
            uint32_t record_num = record.size();
            uint32_t tlv_valid_len = 0;

            if (0 == record_num) {
                return 0;
            }

            tlv_valid_len = TransLogSubTlvEncodeLen * record_num + TransLogHeadTlvEncodeLen;

            for (size_t i = 0; i < record_num; i++) {
                tlv_valid_len += record[i].GetSize();
            }
            return tlv_valid_len;
        }

        rocksdb::Status TransLogRecordStore::WriteOneTlv(char* buf, uint32_t valid_size) {
            rocksdb::Status s;
            TransLogHeadTlv* head_tlv = (TransLogHeadTlv*)buf;

            head_tlv->magic = TRANS_LOG_HEAD_TLV_MAGIC;
            head_tlv->version = 0;
            head_tlv->total_len = (((valid_size + pad_size_ - 1) / pad_size_) * pad_size_);
            head_tlv->pad_len = head_tlv->total_len - valid_size;

            // crc
            AddTlvCRC32(head_tlv);

            // get trans log file writer
            TransLogFileWriter* writer = GetCurrentLogFileWriter();
            if (nullptr == writer) {
                s = rocksdb::Status::NoSpace("get current file writer fail");
            }
            RES_RIF(s);

            const uint32_t max_retry_time = 3;
            uint32_t i = 0;
            s = writer->Append(rocksdb::Slice(buf, head_tlv->total_len), 0);
            while (i < max_retry_time) {
                if (s.ok()) {
                    break;
                }
                index_log() << "Append len:" << head_tlv->total_len << "return:" << s.ToString();

                // if append fail, try to append to a new file
                s = TransLogRecordStore::CloseWriteFile(writer);
                RES_RIF(s);
                writer = GetCurrentLogFileWriter();
                if (nullptr == writer) {
                    s = rocksdb::Status::NoSpace("get current file writer fail");
                }
                RES_RIF(s);
                s = writer->Append(rocksdb::Slice(buf, head_tlv->total_len), 0);
                i++;
            }

            return s;
        }

        // write the list of record to trans log files
        rocksdb::Status TransLogRecordStore::WriteRecordInternal(std::vector<LogRecord>& record) {
            uint32_t tlv_valid_len = GetRecordEncodeLen(record);
            uint32_t one_tlv_len = 0;
            uint32_t buf_size = 0;
            TransLogSubTlv* sub_tlv = nullptr;
            char* buf = nullptr;
            rocksdb::Status s;

            write_lock_.AssertHeld();

            if (0 == tlv_valid_len) {
                return rocksdb::Status::OK();
            }

            // get encode buf
            buf_size = (((tlv_valid_len + pad_size_ - 1) / pad_size_) * pad_size_);
            buf_size = (buf_size > max_buf_size_) ? max_buf_size_ : buf_size;

            auto buffer = mongo::stdx::make_unique<char[]>(buf_size);
            if (nullptr == buffer || nullptr == buffer.get()) {
                s = rocksdb::Status::NoSpace("new file_writer fail");
                return s;
            }
            buf = buffer.get();
            memset(buf, 0, buf_size);

            one_tlv_len = TransLogHeadTlvEncodeLen;
            // encode
            for (size_t i = 0; i < record.size(); i++) {
                if (one_tlv_len + pad_size_ + record[i].GetSize() + TransLogSubTlvEncodeLen >=
                    max_buf_size_) {
                    index_log() << "WriteOneTlv len:" << one_tlv_len << " pad:" << pad_size_
                                << " record.size:" << record[i].GetSize();
                    s = WriteOneTlv(buf, one_tlv_len);
                    if (!s.ok()) {
                        index_err() << "WriteOneTlv len:" << one_tlv_len
                                    << " return:" << s.ToString();
                        break;
                    }
                    static_trans_log_current_size_ += one_tlv_len;
                    memset(buf, 0, buf_size);
                    one_tlv_len = TransLogHeadTlvEncodeLen;
                }

                sub_tlv = (TransLogSubTlv*)(buf + one_tlv_len);
                sub_tlv->record_type = static_cast<uint8_t>(record[i].GetType());
                sub_tlv->len = record[i].GetSize() + TransLogSubTlvEncodeLen;

                memcpy(sub_tlv->data, record[i].GetData(),
                         record[i].GetSize());
                if (record[i].GetLsn() == INVALID_LSN) {
                    sub_tlv->lsn = IncreaseLsn();
                    record[i].SetLsn(sub_tlv->lsn);
                } else {
                    sub_tlv->lsn = record[i].GetLsn();
                }

                one_tlv_len += sub_tlv->len;
            }

            RES_RIF(s);

            s = WriteOneTlv(buf, one_tlv_len);
            RES_RIF(s);

            static_trans_log_current_size_ += one_tlv_len;
            return s;
        }

        rocksdb::Status TransLogRecordStore::WriteRecord(std::vector<LogRecord>& record,
                                                         bool allow_check_point) {
            rocksdb::Status s;

            TranslogMutexLock guard_lock(&write_lock_);

            // wirte checkpoint before current log(s).
            if (allow_check_point) {
                if (MONGO_FAIL_POINT(failToCheckAndWriteCheckPoint)) {
                    index_err() << "Fail point : CheckAndWriteCheckPoint";
                    s = rocksdb::Status::Aborted();
                    RES_RIF(s);
                }
                s = CheckAndWriteCheckPoint();
                RES_RIF(s);
            }

            // write new record
            if (MONGO_FAIL_POINT(failToWriteRecordInternal)) {
                index_err() << "Fail point : WriteRecordInternal";
                s = rocksdb::Status::Aborted();
                RES_RIF(s);
            }

            s = WriteRecordInternal(record);
            RES_RIF(s);

            return rocksdb::Status::OK();
        }

        // write mutilpue records in one write
        rocksdb::Status TransLogRecordStore::WriteRecord(LogRecord& record,
                                                         bool allow_check_point) {
            std::vector<LogRecord> record_list;
            record_list.push_back(record);

            rocksdb::Status s = WriteRecord(record_list, allow_check_point);
            if (s.ok()) {
                record.SetLsn(record_list[0].GetLsn());
            }
            return s;
        }

        // write trans log for checkpoint
        rocksdb::Status TransLogRecordStore::LogCheckPointState(const SystemTlogOperationType type,
                                                                const uint64_t checkpoint_start_lsn,
                                                                uint64_t& new_log_lsn) {
            char buf[64] = {0};
            CheckpointDiskFormat* disk_format = (CheckpointDiskFormat*)buf;

            disk_format->operation_type = static_cast<uint8_t>(type);
            disk_format->checkpoint_start_lsn = checkpoint_start_lsn;
            disk_format->reserve32 = 0;

            LogRecord record(LogRecordType::TransactionLogInternal, buf, CheckpointEncodeLen);
            std::vector<LogRecord> record_list;
            record_list.push_back(record);

            rocksdb::Status s = WriteRecord(record_list, false);
            RES_RIF(s);
            new_log_lsn = record_list[0].GetLsn();

            return s;
        }

        rocksdb::Status TransLogRecordStore::WriteImportantFailLog4CheckpointAbout(void) {
            if (INVALID_LSN == failed_checkpoint_start_lsn_) {
                return rocksdb::Status::OK();
            }
            index_log() << "failed_checkpoint_start_lsn:" << failed_checkpoint_start_lsn_;
            uint64_t lsn;
            rocksdb::Status s = LogCheckPointState(SystemTlogOperationType::AbortCheckpoint,
                                                   failed_checkpoint_start_lsn_, lsn);
            if (s.ok()) {
                failed_checkpoint_start_lsn_ = INVALID_LSN;
                index_log() << "failed_checkpoint_start_lsn set to:" << INVALID_LSN;
            }
            return s;
        }

        ///////////////////////////////TransLogRecordStore init and replay trans
        /// log////////////////////////////////
        //  Register provider in store and call providers InitializationBegin
        rocksdb::Status TransLogRecordStore::RegisterProvider(ILogRecordProvider& provider) {
            ILogRecordProvider* tmp = &provider;
            registered_provider_.push_back(tmp);
            return rocksdb::Status::OK();
        }

        // get  porviders which support record type
        void TransLogRecordStore::GetProvider8RecordType(
            LogReplayOrder replayOrder,
            std::vector<RecordTypeProvider>* record_type_provider_list) {
            rocksdb::Status s;
            RecordTypeProvider record_provider;
            std::vector<LogRecordType> log_record_type_list;

            for (size_t i = 0; i < registered_provider_.size(); i++) {
                log_record_type_list.clear();
                s = registered_provider_[i]->InitializationBegin(replayOrder, log_record_type_list);
                if (!s.ok()) {
                    index_err() << "provider:" << registered_provider_[i]->GetName()
                                << "init begin return:" << s.ToString();
                    continue;
                }
                for (size_t j = 0; j < log_record_type_list.size(); j++) {
                    record_provider.provider = registered_provider_[i];
                    record_provider.type = log_record_type_list[j];
                    record_provider.isNeedMoreRecords = true;
                    record_type_provider_list->push_back(record_provider);
                }
            }
            return;
        }

        /////////////////////////////////////// begin get checkpoint///////////////
        // decode checkpoint for get checkpoint
        bool TransLogRecordStore::DecodeCheckpoint(const uint64_t file_sn,
                                                   const uint64_t offset_in_file,
                                                   const TransLogSubTlv* sub_tlv,
                                                   bool& get_checkpoint) {
            CheckpointDiskFormat* checkpoint = nullptr;

            get_checkpoint = false;
            if (static_cast<uint8_t>(LogRecordType::TransactionLogInternal) !=
                sub_tlv->record_type) {
                index_err() << "invalid sub tlv,record-type:"
                            << static_cast<uint32_t>(sub_tlv->record_type)
                            << " LSN:" << sub_tlv->lsn;
                return false;
            }

            bool ret = true;
            CheckpointLocation invalid_checkpoint;
            checkpoint = (CheckpointDiskFormat*)sub_tlv->data;
            if (static_cast<uint8_t>(SystemTlogOperationType::StartCheckpoint) ==
                checkpoint->operation_type) {
                // start checkpoint
                if (IsCheckpointLocationValid(checkpoint_commit_) &&
                    sub_tlv->lsn == checkpoint_commit_.checkpoint_start_lsn) {
                    checkpoint_commit_.checkpoint_start_file_sn = file_sn;
                    checkpoint_commit_.checkpoint_start_offset = offset_in_file;
                    get_checkpoint = true;
                } else {
                    // try to find checkpoint_abort
                    size_t i = 0;
                    for (i = 0; i < invalid_checkpoint_list_.size(); i++) {
                        if (sub_tlv->lsn == invalid_checkpoint_list_[i].checkpoint_start_lsn) {
                            // found checkpoint_abort
                            invalid_checkpoint_list_[i].checkpoint_start_file_sn = file_sn;
                            invalid_checkpoint_list_[i].checkpoint_start_offset = offset_in_file;
                            break;
                        }
                    }

                    if (i >= invalid_checkpoint_list_.size()) {
                        // not found checkpoint_abort
                        invalid_checkpoint.checkpoint_start_file_sn = file_sn;
                        invalid_checkpoint.checkpoint_start_offset = offset_in_file;
                        invalid_checkpoint.checkpoint_start_lsn = sub_tlv->lsn;
                        invalid_checkpoint.self_file_sn = INVALID_FILE_SN;
                        invalid_checkpoint.self_lsn = INVALID_LSN;
                        invalid_checkpoint.self_offset = 0;
                        invalid_checkpoint_list_.push_back(invalid_checkpoint);
                    }
                }
                ret = true;
            } else if (static_cast<uint8_t>(SystemTlogOperationType::AbortCheckpoint) ==
                       checkpoint->operation_type) {
                invalid_checkpoint.self_file_sn = file_sn;
                invalid_checkpoint.self_lsn = sub_tlv->lsn;
                invalid_checkpoint.self_offset = offset_in_file;
                invalid_checkpoint.checkpoint_start_file_sn = INVALID_FILE_SN;
                invalid_checkpoint.checkpoint_start_offset = 0;
                invalid_checkpoint.checkpoint_start_lsn = checkpoint->checkpoint_start_lsn;
                invalid_checkpoint_list_.push_back(invalid_checkpoint);
                ret = true;
            } else if (static_cast<uint8_t>(SystemTlogOperationType::CommitCheckpoint) ==
                       checkpoint->operation_type) {
                checkpoint_commit_.checkpoint_start_file_sn = INVALID_FILE_SN;
                checkpoint_commit_.self_file_sn = file_sn;
                checkpoint_commit_.checkpoint_start_lsn = checkpoint->checkpoint_start_lsn;
                checkpoint_commit_.self_lsn = sub_tlv->lsn;
                checkpoint_commit_.self_offset = offset_in_file;
                ret = true;
            } else {
                index_err() << "invalid sub tlv,record-type:"
                            << static_cast<uint32_t>(sub_tlv->record_type)
                            << " LSN:" << sub_tlv->lsn
                            << " O:" << static_cast<uint32_t>(checkpoint->operation_type);
                ret = false;
            }
            index_log() << "get checkpoint_commit_:file sn "
                        << checkpoint_commit_.checkpoint_start_file_sn << ","
                        << checkpoint_commit_.self_file_sn << " :lsn "
                        << checkpoint_commit_.checkpoint_start_lsn << " ,"
                        << checkpoint_commit_.self_lsn << " :offset "
                        << checkpoint_commit_.checkpoint_start_offset << " ,"
                        << checkpoint_commit_.self_offset << " ret:" << ret
                        << " get_checkpoint:" << get_checkpoint;

            for (size_t i = 0; i < invalid_checkpoint_list_.size(); i++) {
                index_log() << "invalid checkpoint:" << i
                            << " file_sn:" << invalid_checkpoint_list_[i].checkpoint_start_file_sn
                            << "," << invalid_checkpoint_list_[i].self_file_sn
                            << " lsn:" << invalid_checkpoint_list_[i].checkpoint_start_lsn << " ,"
                            << invalid_checkpoint_list_[i].self_lsn
                            << " offset:" << invalid_checkpoint_list_[i].checkpoint_start_offset
                            << "," << invalid_checkpoint_list_[i].self_offset;
            }
            return ret;
        }

        // find checkpoint in inverted order for get checkpoint
        // if find checkpoint start adn checkpoint commint , return true; otherwise, return false
        bool TransLogRecordStore::InvertGetCheckpoint(const uint64_t file_sn,
                                                      const uint64_t offset_in_file,
                                                      const rocksdb::Slice& data,
                                                      bool& get_checkpoint, uint32_t& decode_len) {
            int64_t decode_offset = 0;
            const char* buf = data.data();
            TransLogHeadTlv* head_tlv = nullptr;
            TransLogSubTlv* sub_tlv = nullptr;
            decode_len = 0;
            // find checkpoint in inverted order
            decode_offset = data.size() > pad_size_ ? (data.size() - pad_size_) : 0;
            while (decode_offset >= 0) {
                head_tlv = (TransLogHeadTlv*)(buf + decode_offset);
                if (TRANS_LOG_HEAD_TLV_MAGIC == head_tlv->magic) {
                    if (head_tlv->total_len + decode_offset > static_cast<int64_t>(data.size())) {
                        // the left space cannot put down a full TransLogHeadTlv
                        index_err() << " decode_offset:" << decode_offset
                                    << " data.size:" << data.size()
                                    << " total_len:" << head_tlv->total_len;
                        break;
                    }
                    decode_len += head_tlv->total_len;
                    // crc check
                    if (true != CheckTlvCrc(head_tlv)) {
                        index_err() << "TransLogHeadTlv crc err,decode_offset:" << decode_offset
                                    << " head_tlv->total_len:" << head_tlv->total_len
                                    << " data.size:" << data.size() << " decode_len:" << decode_len;
                        return false;
                    }
                    sub_tlv = (TransLogSubTlv*)head_tlv->data;
                    if (static_cast<uint8_t>(LogRecordType::TransactionLogInternal) ==
                        sub_tlv->record_type) {
                        if (!DecodeCheckpoint(file_sn, offset_in_file + decode_offset, sub_tlv,
                                              get_checkpoint)) {
                            return false;
                        }
                        if (get_checkpoint) {
                            return true;
                        }
                    }
                }
                decode_offset -= pad_size_;
            }

            return true;
        }

        // read data from end to begin in one trans log file for get checkpoint
        rocksdb::Status TransLogRecordStore::InvertReadTransLogFils(const uint64_t file_sn,
                                                                    char* buf,
                                                                    const uint32_t buf_len,
                                                                    bool& get_checkpoint) {
            std::string file_name = MakeFileName(file_sn);
            rocksdb::Status s;
            uint64_t file_size = 0;

            get_checkpoint = false;
            s = env_->GetFileSize(file_name, &file_size);
            if (!s.ok()) {
                s = rocksdb::Status::NotFound("file not exist", file_name);
                return s;
            }

            if (0 == file_size) {
                index_err() << " file:" << file_name << " size is zero";
                return rocksdb::Status::OK();
            }

            std::unique_ptr<rocksdb::RandomAccessFile> random_file;
            s = env_->NewRandomAccessFile(file_name, &random_file, rocksdb::EnvOptions());
            RES_RIF(s);

            rocksdb::Slice data;
            int64_t read_offset =
                static_cast<int64_t>(file_size > buf_len ? (file_size - buf_len) : 0);
            size_t read_len = static_cast<size_t>(file_size > buf_len ? buf_len : file_size);
            uint64_t already_decode_len = 0;
            uint32_t tmp_decode_len = 0;
            do {
                // read data from file
                s = random_file->Read(read_offset, read_len, &data, buf);
                if (!s.ok() || 0 == data.size()) {
                    index_err() << " file:" << file_name << " offset:" << read_offset
                                << " read_len:" << read_len << "return:" << s.ToString();

                    s = rocksdb::Status::IOError(file_name, "read err");
                    return s;
                }

                // decode
                tmp_decode_len = 0;
                if (!InvertGetCheckpoint(file_sn, read_offset, data, get_checkpoint,
                                         tmp_decode_len)) {
                    // decode fail
                    index_err() << " file:" << file_name << " offset:" << read_offset
                                << " read_len:" << read_len << " tmp_decode:" << tmp_decode_len
                                << " already_decode:" << already_decode_len
                                << " file_size:" << file_size << " decode fail";
                    s = rocksdb::Status::IOError(file_name, "decode fail");
                    return s;
                }
                if (get_checkpoint) {
                    break;
                }
                already_decode_len += tmp_decode_len;
                if (already_decode_len >= file_size) {
                    index_LOG(2) << " file:" << file_name << " offset:" << read_offset
                                 << " read_len:" << read_len << " tmp_decode:" << tmp_decode_len
                                 << " already_decode:" << already_decode_len
                                 << " file_size:" << file_size;
                    break;
                }
                read_offset = file_size - already_decode_len;
                read_len = static_cast<size_t>(read_offset > buf_len ? buf_len : read_offset);
                read_offset -= read_len;

                index_log() << " file:" << file_name << " offset:" << read_offset
                            << " read_len:" << read_len << " tmp_decode:" << tmp_decode_len
                            << " already_decode:" << already_decode_len
                            << " file_size:" << file_size;

            } while (read_len > 0);

            return rocksdb::Status::OK();
        }

        // get checkpoint by read data from the end to beginning.
        // if there are no trans log file , need_replay return false; otherwise, need_replay return
        // true
        rocksdb::Status TransLogRecordStore::GetCheckpoint4Replay(bool& need_replay) {
            // get trans log file list by readdir
            std::vector<std::string> file_list;
            rocksdb::Status s;
            uint64_t file_sn = 0;
            uint64_t last_max_file_sn = 0;
            size_t i = 0;
            TransLogFileMetaData file_metadata = {0, 0, 0};
            write_lock_.AssertHeld();

            index_log() << "TransLogRecordStore::GetCheckpoint4Replay()-> path: "
                        << trans_log_path_;
            s = env_->GetChildren(trans_log_path_, &file_list);
            if (rocksdb::Status::NotFound() == s || (s.ok() && 0 == file_list.size())) {
                // open new db
                index_log() << "no any trans log";
                need_replay = false;
                return rocksdb::Status::OK();
            }

            RES_RIF(s);

            // store alive trans log file into alive_log_files_
            for (i = 0; i < file_list.size(); i++) {
                if (ParseFileName(file_list[i].c_str(), &file_sn)) {
                    file_metadata.file_sn = file_sn;
                    alive_log_files_.push_back(file_metadata);
                    if (file_sn > last_max_file_sn) {
                        last_max_file_sn = file_sn;
                    }
                }
            }

            if (0 == alive_log_files_.size()) {
                // open new db
                index_log() << "no any trans log";
                need_replay = false;
                return rocksdb::Status::OK();
            }

            // sort files from small to large
            std::sort(alive_log_files_.begin(), alive_log_files_.end(), CompareTransLogFileSn);

            for (i = 0; i < alive_log_files_.size(); i++) {
                index_log() << "alive_log_files:" << i << " file_sn:" << alive_log_files_[i].file_sn
                            << " file_size:" << alive_log_files_[i].file_size
                            << " largest_lsn:" << alive_log_files_[i].largest_lsn;
            }

            // read file from the largest file_sn to the smallest file_sn
            bool get_checkpoint = false;
            // read 1M data at one time
            auto buffer = mongo::stdx::make_unique<char[]>(max_buf_size_);
            if (nullptr == buffer || nullptr == buffer.get()) {
                s = rocksdb::Status::NoSpace("new file_writer fail");
                return s;
            }
            memset(buffer.get(), 0, max_buf_size_);
            for (i = alive_log_files_.size(); i > 0; i--) {
                s = InvertReadTransLogFils(alive_log_files_[i - 1].file_sn, buffer.get(),
                                           max_buf_size_, get_checkpoint);
                RES_RIF(s);

                if (get_checkpoint) {
                    break;
                }
            }
            need_replay = true;

            next_file_sn_ = last_max_file_sn + 1;
            index_LOG(0)
                << "[transLog] TransLogRecordStore::GetCheckpoint4Replay()-> next_file_sn_: "
                << next_file_sn_.load() << "; last_max_file_sn: " << last_max_file_sn;
            return rocksdb::Status::OK();
        }
        ///////////////////////////////////////end of get checkpoint///////////////

        ////////////////////////////////////// begin replay trans log///////////////
        // get the start position and end position for replay trans log
        // if return flase, it means trans log is Corruption, needn't repaly trans log
        bool TransLogRecordStore::GetTransLogReplayStartAndEndPos(
            std::vector<ValidStartEndPosition>* valid_position_list) {
            index_log() << "checkpoint_commit_:file sn "
                        << checkpoint_commit_.checkpoint_start_file_sn << " ,"
                        << checkpoint_commit_.self_file_sn << " :lsn "
                        << checkpoint_commit_.checkpoint_start_lsn << " ,"
                        << checkpoint_commit_.self_lsn << " :offset "
                        << checkpoint_commit_.checkpoint_start_offset << " ,"
                        << checkpoint_commit_.self_offset;

            ValidStartEndPosition valid_position = {INVALID_FILE_SN, 0, 0};
            uint64_t valid_begin_file_sn = INVALID_FILE_SN;
            uint32_t valid_begin_offset = 0;

            if (0 == alive_log_files_.size()) {
                index_log() << "alive_log_files_ is empty:";
                return true;
            }

            // get start position of valid checkpoint
            if (IsCheckpointLocationValid(checkpoint_commit_)) {
                valid_begin_file_sn = checkpoint_commit_.checkpoint_start_file_sn;
                valid_begin_offset = checkpoint_commit_.checkpoint_start_offset;
            } else {
                // checkpoit_commit is invalid
                valid_begin_file_sn = alive_log_files_.begin()->file_sn;
                index_LOG(0) << "[transLog] TransLogRecordStore::GetTransLogReplayStartAndEndPos "
                                "valid_begin_file_sn: "
                             << valid_begin_file_sn;
                valid_begin_offset = 0;
            }

            size_t i = 0;
            std::vector<CheckpointLocation> invalid_checkpoint_list = invalid_checkpoint_list_;
            index_LOG(0) << "[transLog] TransLogRecordStore::GetTransLogReplayStartAndEndPos "
                            "invalid_checkpoint_list size: "
                         << invalid_checkpoint_list.size();
            std::vector<TransLogFileMetaData> alive_log_files = alive_log_files_;

            // skip files which file_sn is smaller than start_file_sn
            auto it = alive_log_files.begin();
            while (it != alive_log_files.end()) {
                if (it->file_sn < valid_begin_file_sn) {
                    it = alive_log_files.erase(it);
                } else {
                    it++;
                }
            }

            for (i = 0; i < invalid_checkpoint_list_.size(); i++) {
                index_log() << "invalid checkpoint:" << i
                            << " file_sn:" << invalid_checkpoint_list_[i].checkpoint_start_file_sn
                            << " ," << invalid_checkpoint_list_[i].self_file_sn
                            << " lsn:" << invalid_checkpoint_list_[i].checkpoint_start_lsn << " ,"
                            << invalid_checkpoint_list_[i].self_lsn
                            << " offset:" << invalid_checkpoint_list_[i].checkpoint_start_offset
                            << " ," << invalid_checkpoint_list_[i].self_offset;
            }

            for (i = 0; i < alive_log_files.size(); i++) {
                index_log() << "alive_log_files:" << i << " file_sn:" << alive_log_files[i].file_sn
                            << " file_size:" << alive_log_files[i].file_size
                            << " lsn:" << alive_log_files[i].largest_lsn;
            }

            // valid_position sort in reverse order, from large to small
            //                 valid_begin_file_sn  checkpoint_start_file_sn   self_file_sn
            //                         |                     |                    |
            //    | invalid checkpoint |                     |invalid checkpoint  |
            //---------------------------------------------------------------------------->alive_log_files
            for (i = invalid_checkpoint_list.size(); i > 0; i--) {
                do {
                    // p1-- skip files which file_sn is smaller than valid_begin_file_sn
                    if (alive_log_files.begin()->file_sn < valid_begin_file_sn) {
                        alive_log_files.erase(alive_log_files.begin());
                        continue;
                    }
                    // p2--  files which file_sn is smaller than
                    // invalid_checkpoint_list[i].checkpoint_start_file_sn,
                    // and larger than valid_begin_file_sn, add to valid_position
                    else if (alive_log_files.begin()->file_sn <
                             invalid_checkpoint_list[i - 1].checkpoint_start_file_sn) {
                        valid_position.file_sn = alive_log_files.begin()->file_sn;
                        valid_position.start_offset =
                            (valid_position.file_sn == valid_begin_file_sn) ? valid_begin_offset
                                                                            : 0;
                        // MAX_FILE_OFFSET, it means the end of the file
                        valid_position.end_offset =
                            MAX_FILE_OFFSET;  // alive_log_files.begin()->file_size;
                        valid_position_list->push_back(valid_position);
                        alive_log_files.erase(alive_log_files.begin());
                        continue;
                    }
                    // p3--  files which file_sn is equal to
                    // invalid_checkpoint_list[i].checkpoint_start_file_sn
                    else if (alive_log_files.begin()->file_sn ==
                             invalid_checkpoint_list[i - 1].checkpoint_start_file_sn) {
                        valid_position.file_sn = alive_log_files.begin()->file_sn;
                        valid_position.start_offset =
                            (valid_position.file_sn == valid_begin_file_sn) ? valid_begin_offset
                                                                            : 0;
                        valid_position.end_offset =
                            invalid_checkpoint_list[i - 1].checkpoint_start_offset;
                        valid_begin_file_sn = invalid_checkpoint_list[i - 1].self_file_sn;
                        valid_begin_offset = invalid_checkpoint_list[i - 1].self_offset;
                        // if two invalid checkpoint are back to back
                        valid_position_list->push_back(valid_position);
                        // alive_log_files.erase(alive_log_files.begin());
                        break;
                    } else {
                        // Corruption
                        return false;
                    }

                } while (alive_log_files.size() != 0);
            }

            // files which file_sn is larger than all invalid-checkpoint
            for (i = 0; i < alive_log_files.size(); i++) {
                if (alive_log_files[i].file_sn < valid_begin_file_sn) {
                    // file has been processed in the p3-- step
                    continue;
                }
                valid_position.file_sn = alive_log_files[i].file_sn;
                valid_position.start_offset =
                    (valid_position.file_sn == valid_begin_file_sn) ? valid_begin_offset : 0;
                valid_position.end_offset = MAX_FILE_OFFSET;
                // if two invalid checkpoint are back to back
                valid_position_list->push_back(valid_position);
                index_LOG(0) << "[transLog] TransLogRecordStore::GetTransLogReplayStartAndEndPos "
                                "valid_position_list file_sn: "
                             << valid_position.file_sn;
            }

            index_LOG(0) << "[transLog] TransLogRecordStore::GetTransLogReplayStartAndEndPos "
                            "valid_position_list num: "
                         << valid_position_list->size();

            return (valid_position_list->size() > 0);
        }

        // Forward replay trans log
        rocksdb::Status TransLogRecordStore::ForwardReplayTransLog(
            std::vector<RecordTypeProvider>& record_type_provider, uint64_t& max_replayed_lsn) {
            rocksdb::Status s;
            std::string file_name;
            std::vector<ValidStartEndPosition> position;
            write_lock_.AssertHeld();

            if (!GetTransLogReplayStartAndEndPos(&position) && 0 == position.size()) {
                s = rocksdb::Status::Corruption("get trans log position fail");
                return s;
            }

            if (0 == position.size()) {
                index_log() << "Valid Position is empty:";
                return rocksdb::Status::OK();
            }

            index_LOG(0) << "Valid Position num:" << position.size();
            for (size_t i = 0; i < position.size(); i++) {
                index_LOG(0) << "Valid Position:" << i << " file_sn:" << position[i].file_sn
                             << " start-offset:" << position[i].start_offset
                             << " end-offset:" << position[i].end_offset;
            }

            uint64_t file_size = 0;
            auto iter = position.begin();
            for (; iter != position.end(); iter++) {
                file_name = MakeFileName(iter->file_sn);
                s = env_->GetFileSize(file_name, &file_size);
                if (!s.ok()) {
                    index_err() << "GetFileSize file: " << file_name << "; error: " << s.ToString();
                    s = rocksdb::Status::NotFound("file(%s)not exist", file_name);
                    return s;
                }
                if (0 == file_size) {
                    index_err() << "forward file:" << file_name << " size is zero";
                    continue;
                }

                if (iter->start_offset >= file_size) {
                    // If the right boundary of an invalid positions is exactly aligned
                    // with the right boundary of the file that it belongs to, the above
                    // method GetTransLogReplayStartAndEndPos() will produce an empty
                    // valid position which will be skipped here.
                    index_warning() << "forward file:" << file_name << " size:" << file_size
                                    << "start offset:" << iter->start_offset;
                    continue;
                }

                s = ForwardReadOneTransLogFile(record_type_provider, iter->file_sn,
                                               iter->start_offset, iter->end_offset, file_size,
                                               file_name, max_replayed_lsn);
                RES_RIF(s);
                static_trans_log_current_size_ += file_size;
            }

            return rocksdb::Status::OK();
        }

        // decode one TransLogHeadTlv
        rocksdb::Status TransLogRecordStore::DecodeOneHeadTlv(
            std::vector<RecordTypeProvider>& record_type_provider, const TransLogHeadTlv* head_tlv,
            uint32_t& already_decode_len, uint64_t& max_replayed_lsn) {
            TransLogSubTlv* sub_tlv = nullptr;
            bool needMoreRecords = true;

            already_decode_len = 0;
            // crc check
            if (true != CheckTlvCrc(head_tlv)) {
                index_err() << "CheckTlvCrc err!";
                return rocksdb::Status::Corruption("TransLogHeadTlv crc err");
            }
            index_LOG(2) << " replay one tlv:total_len" << head_tlv->total_len;

            // check duplicate LogRecord
            // Maybe the upper layer didn't get the acknowledge from the underlying layer which has
            // written a head_tlv successfully actually. On this case, the upper layer will rewrite
            // the head_tlv and it can result in duplicated data. Here we ignore the duplicated data
            // by comparing the lsn of the first sub_tlv of the two adjacent head_tlv.
            sub_tlv = (TransLogSubTlv*)((char*)(head_tlv->data) + already_decode_len);
            if (duplicate_lsn_ == sub_tlv->lsn && duplicate_crc_ == head_tlv->crc32) {
                index_err() << " duplicate log sn : " << sub_tlv->lsn
                            << ", crc32:" << head_tlv->crc32;
                already_decode_len = head_tlv->total_len;
                return rocksdb::Status::OK();
            }
            duplicate_lsn_ = sub_tlv->lsn;
            duplicate_crc_ = head_tlv->crc32;

            while (already_decode_len + TransLogHeadTlvEncodeLen <
                   (head_tlv->total_len - head_tlv->pad_len)) {
                sub_tlv = (TransLogSubTlv*)((char*)(head_tlv->data) + already_decode_len);
                if ((size_t)sub_tlv->len <= TransLogSubTlvEncodeLen) {
                    index_err() << " sub_tlv->len:" << sub_tlv->len << " TransLogHeadTlvEncodeLen"
                                << TransLogHeadTlvEncodeLen;
                    return rocksdb::Status::Corruption("sub_tlv->len <=  TransLogHeadTlvEncodeLen");
                }

                if (sub_tlv->record_type ==
                    static_cast<uint8_t>(LogRecordType::TransactionLogInternal)) {
                    // skip LogRecordType::TransactionLogInternal
                    index_log() << " skip record of system, type:"
                                << static_cast<char>(sub_tlv->record_type) << " total_len"
                                << head_tlv->total_len;
                    already_decode_len += sub_tlv->len;
                    continue;
                }

                // get provider according to LogRecord Type
                ILogRecordProvider* provider =
                    GetRecordProvider(static_cast<LogRecordType>(sub_tlv->record_type),
                                      record_type_provider, needMoreRecords);
                if (nullptr == provider) {
                    index_err() << " get provider fail:"
                                << static_cast<uint32_t>(sub_tlv->record_type);
                    return rocksdb::Status::Corruption("get provider fail");
                }

                // judge needMoreRecords in initial provider
                if (!needMoreRecords) {
                    index_log() << " provider need no records and continue:"
                                << static_cast<uint32_t>(sub_tlv->record_type);
                    already_decode_len += sub_tlv->len;
                    continue;
                }

                // replay
                LogRecord record((LogRecordType)sub_tlv->record_type, sub_tlv->lsn,
                                 (char*)sub_tlv->data,
                                 (size_t)(sub_tlv->len - TransLogSubTlvEncodeLen));
                rocksdb::Status s = provider->ReplayLogRecord(record, needMoreRecords);
                RES_RIF(s);

                if (sub_tlv->lsn > max_replayed_lsn) {
                    max_replayed_lsn = sub_tlv->lsn;
                }

                if (!needMoreRecords) {
                    index_log() << " provider need no more records and return:"
                                << static_cast<uint32_t>(sub_tlv->record_type);
                    SaveRecordProviderFlag(static_cast<LogRecordType>(sub_tlv->record_type),
                                           record_type_provider, needMoreRecords);
                }
                already_decode_len += sub_tlv->len;
            }
            already_decode_len = head_tlv->total_len;
            return rocksdb::Status::OK();
        }

        // decode one slice which's length is not larger than max_buf_size_.
        rocksdb::Status TransLogRecordStore::DecodeOneSlice(
            const rocksdb::Slice& data, std::vector<RecordTypeProvider>& record_type_provider,
            uint32_t& decode_len, uint64_t& max_replayed_lsn) {
            uint32_t decode_one_tlv_len = 0;
            TransLogHeadTlv* head_tlv = nullptr;
            rocksdb::Status s;

            decode_len = 0;
            while (decode_len < data.size()) {
                head_tlv = (struct TransLogHeadTlv*)(data.data() + decode_len);

                // Check the magic of transaction log head tlv.
                if (TRANS_LOG_HEAD_TLV_MAGIC != head_tlv->magic) {
                    index_err() << " TransLogHeadTlv corruption, magic:" << head_tlv->magic;
                    s = rocksdb::Status::Corruption("new file_writer fail");
                    RES_RIF(s);
                }

                // Check if the left space can put down a full transaction log head tlv.
                if (head_tlv->total_len > (data.size() - decode_len)) {
                    // the left space cannot put down a full TransLogHeadTlv
                    index_err() << " decode_len:" << decode_len << " data.size:" << data.size()
                                << " total_len:" << head_tlv->total_len;
                    break;
                }

                // Decode one transaction log head tlv.
                s = DecodeOneHeadTlv(record_type_provider, head_tlv, decode_one_tlv_len,
                                     max_replayed_lsn);
                RES_RIF(s);
                index_LOG(3) << " one tlv decode_len:" << decode_len << " data.size:" << data.size()
                             << " total_len:" << head_tlv->total_len;

                decode_len += decode_one_tlv_len;
            }

            return rocksdb::Status::OK();
        }

        // read data from start_offset to end_offset in one trans log file for get checkpoint
        rocksdb::Status TransLogRecordStore::ForwardReadOneTransLogFile(
            std::vector<RecordTypeProvider>& record_type_provider, const uint64_t file_sn,
            uint32_t start_offset, const uint32_t end_offset, const uint64_t file_size,
            std::string file_name, uint64_t& max_replayed_lsn) {
            // read 1M data at one time
            rocksdb::Slice data;
            size_t read_len = max_buf_size_;
            std::unique_ptr<rocksdb::RandomAccessFile> seq_file;
            rocksdb::Status s;

            auto buffer = mongo::stdx::make_unique<char[]>(max_buf_size_);
            if (nullptr == buffer || nullptr == buffer.get()) {
                s = rocksdb::Status::NoSpace("new file_writer fail");
                return s;
            }

            memset(buffer.get(), 0, max_buf_size_);

            s = env_->NewRandomAccessFile(file_name, &seq_file, rocksdb::EnvOptions());
            RES_RIF(s);
            uint32_t end_pos =
                static_cast<uint32_t>(file_size > end_offset ? end_offset : file_size);
            // Read til the end of the current TransLogFile
            while (start_offset < end_pos) {
                read_len = static_cast<size_t>(
                    (end_pos - start_offset) > read_len ? read_len : (end_pos - start_offset));
                s = seq_file->Read(start_offset, read_len, &data, buffer.get());
                if (!s.ok() || 0 == data.size()) {
                    index_err() << " file:" << file_name << " offset:" << start_offset
                                << " read_len:" << read_len << "return:" << s.ToString();
                    s = rocksdb::Status::IOError(file_name, "read err");
                    return s;
                }

                uint32_t decode_len = 0;
                s = DecodeOneSlice(data, record_type_provider, decode_len, max_replayed_lsn);
                RES_RIF(s);
                start_offset += decode_len;
                index_LOG(2) << " read buf :start_offset" << start_offset << " end_pos:" << end_pos;
            }

            return rocksdb::Status::OK();
        }

        // Get Record Porviders according to record_type
        ILogRecordProvider* TransLogRecordStore::GetRecordProvider(
            const LogRecordType type, std::vector<RecordTypeProvider>& record_type_provider_list,
            bool& needMoreRecords) {
            auto iter = record_type_provider_list.begin();
            for (; iter != record_type_provider_list.end(); iter++) {
                if (type == iter->type) {
                    needMoreRecords = iter->isNeedMoreRecords;
                    return iter->provider;
                }
            }
            index_log() << "provider has not been found:" << static_cast<uint32_t>(type);
            return nullptr;
        }

        // Save Record Porviders flag according to record_type
        void TransLogRecordStore::SaveRecordProviderFlag(
            const LogRecordType type, std::vector<RecordTypeProvider>& record_type_provider_list,
            bool& needMoreRecords) {
            auto iter = record_type_provider_list.begin();
            for (; iter != record_type_provider_list.end(); iter++) {
                if (type == iter->type) {
                    iter->isNeedMoreRecords = needMoreRecords;
                }
            }

            index_log() << "provider has not been found:" << static_cast<uint32_t>(type);
            return;
        }

        rocksdb::Status TransLogRecordStore::CloseAllWriteFile() {
            rocksdb::Status s;
            auto iter = writable_files_.begin();
            for (; iter != writable_files_.end(); iter++) {
                s = (*iter)->Close();
                RES_RIF(s);
            }
            return s;
        }

        uint64_t TransLogRecordStore::GetMinFileSn() {
            uint64_t file_sn = 0;

            std::lock_guard<std::mutex> lock(log_file_lock_);
            if (0 == alive_log_files_.size()) {
                return 0;
            }
            file_sn = alive_log_files_[0].file_sn;
            for (size_t i = 0; i < alive_log_files_.size(); i++) {
                if (alive_log_files_[i].file_sn < file_sn) {
                    file_sn = alive_log_files_[i].file_sn;
                }
            }
            return file_sn;
        }

        void TransLogRecordStore::ClearTransLogFiles(void) {
            return;
            std::vector<std::string> file_list;
            rocksdb::Status s;
            uint64_t file_sn = 0;
            uint64_t min_file_sn = 0;
            size_t i = 0;

            min_file_sn = GetMinFileSn();
            if (0 == min_file_sn) {
                // open new db
                index_log() << "alive_log_files_";
                return;
            }

            s = env_->GetChildren(trans_log_path_, &file_list);
            if (!s.ok()) {
                index_err() << "GetChildren:" << trans_log_path_ << " return:" << s.ToString();
                return;
            }

            if (0 == file_list.size()) {
                // open new db
                index_err() << "no any trans log";
                return;
            }

            // store alive trans log file into alive_log_files_
            for (i = 0; i < file_list.size(); i++) {
                if (!ParseFileName(file_list[i].c_str(), &file_sn)) {
                    index_err() << "unkown file name:" << file_list[i];
                }

                // delay to delete
                if (file_sn + 2 < min_file_sn) {
                    s = deleteFile(file_list[i]);
                    index_err() << "DeleteFile:" << file_list[i] << " return:" << s.ToString();
                }
            }
        }

        ////////////////////////////////////// end of replay trans log///////////////

        rocksdb::Status TransLogRecordStore::Init(void) {
            index_log() << "TransLogRecordStore::Init()";
            std::vector<RecordTypeProvider> record_type_provider;
            rocksdb::Status s;

            TranslogMutexLock guard_lock(&write_lock_);

            static_trans_log_current_size_ = 0;
            static_trans_log_old_size_ = 0;

            // get record-types and providers which needs support
            GetProvider8RecordType(LogReplayOrder::Forward, &record_type_provider);
            if (0 == record_type_provider.size()) {
                index_log() << "no record-types";
                return rocksdb::Status::Corruption();
            }

            // get checkpoint by read data from the end to beginning.
            bool need_replay = false;
            index_log() << "TransLogRecordStore::Init() GetCheckpoint4Replay";
            s = TransLogRecordStore::GetCheckpoint4Replay(need_replay);
            RES_RIF(s);

            uint64_t max_replayed_lsn = 0;
            // Forward replay trans log
            if (need_replay) {
                index_log() << "TransLogRecordStore::Init() need replay";
                s = TransLogRecordStore::ForwardReplayTransLog(record_type_provider,
                                                               max_replayed_lsn);
                RES_RIF(s);
                SetLsn(max_replayed_lsn + 100);
            }

            static_trans_log_old_size_ = static_trans_log_current_size_.load();

            // notify replay end
            ILogRecordStore* store = (ILogRecordStore*)(this);
            record_writer_.Init(store);
            for (size_t i = 0; i < registered_provider_.size(); i++) {
                index_log()
                    << "TransLogRecordStore::Init() registered_provider_[i]->InitializationEnd()";
                s = registered_provider_[i]->InitializationEnd(record_writer_);
                if (!s.ok()) {
                    index_err() << "provider:" << registered_provider_[i]->GetName()
                                << "init end return:" << s.ToString();
                    continue;
                }
            }

            ClearTransLogFiles();

            return rocksdb::Status::OK();
        }

        ////////////////////////////////////// write check point ///////////////
        rocksdb::Status TransLogRecordStore::WriteCheckPoint(bool realloc_lsn) {
            uint64_t current_trans_log_total_size = static_trans_log_current_size_;
            uint64_t current_file_sn = INVALID_FILE_SN;
            size_t i = 0;
            rocksdb::Status s;
            uint32_t provider_num = registered_provider_.size();
            uint64_t checkpoint_start_lsn = 0;
            uint64_t lsn = 0;
            // index for provider, In exception handling, the providers from
            // provider_index_need_notify_end,
            // need to notify CheckpointEnd
            uint32_t provider_index_need_notify_end = std::numeric_limits<uint32_t>::max();

            if (alive_log_files_.size() > 0) {
                current_file_sn = alive_log_files_[alive_log_files_.size() - 1].file_sn;
            }

            // write trans log for checkpoint start
            s = LogCheckPointState(SystemTlogOperationType::StartCheckpoint, 0,
                                   checkpoint_start_lsn);
            if (!s.ok()) {
                index_err() << "log checkpoint start fail, return:" << s.ToString();
                return s;
            }
            index_log() << "log checkpoint start success, lsn:" << checkpoint_start_lsn;

            do {
                // notify checkpoint start
                for (i = 0; i < provider_num; i++) {
                    registered_provider_[i]->CheckpointBegin();
                }
                provider_index_need_notify_end = i;

                // write log for checkpoint
                for (i = 0; i < provider_num; i++) {
                    s = registered_provider_[i]->WriteCheckpoint(realloc_lsn);
                    if (!s.ok()) {
                        index_err() << "provider:" << registered_provider_[i]->GetName()
                                    << "WriteCheckpoint return:" << s.ToString();
                        provider_index_need_notify_end = i;
                        break;
                    }
                }

                // write trans log for checkpoint commit
                s = LogCheckPointState(SystemTlogOperationType::CommitCheckpoint,
                                       checkpoint_start_lsn, lsn);
                if (!s.ok()) {
                    index_err() << "log checkpoint start fail, return:" << s.ToString();
                    break;
                }

                // notify checkpoint end
                for (i = 0; i < provider_num; i++) {
                    registered_provider_[i]->CheckpointEnd();
                }

                index_log() << "log checkpoint Commit success, start lsn:" << checkpoint_start_lsn
                            << " commit lsn:" << lsn;

            } while (0);

            if (s.ok()) {
                static_trans_log_current_size_ =
                    static_trans_log_current_size_ - current_trans_log_total_size;
                static_trans_log_old_size_ = static_trans_log_current_size_.load();

                // try to delete old files
                if (current_file_sn != INVALID_FILE_SN) {
                    DeleteSmallerWriteFile(current_file_sn);
                }
            } else {
                rocksdb::Status fail_status;
                // todo write trans log for checkpoint abourt
                fail_status = LogCheckPointState(SystemTlogOperationType::AbortCheckpoint,
                                                 checkpoint_start_lsn, lsn);
                if (!fail_status.ok()) {
                    index_err() << "log checkpoint abort fail, return:" << s.ToString();
                    failed_checkpoint_start_lsn_ = checkpoint_start_lsn;
                }
                // notify checkpoint end
                for (i = 0; i < provider_index_need_notify_end; i++) {
                    registered_provider_[i]->CheckpointEnd();
                }
                index_log() << "log checkpoint abort success, start lsn:" << checkpoint_start_lsn
                            << " abort lsn:" << lsn;
            }

            return s;
        }

        ////////////////////////////////////// check and write check point ///////////////
        rocksdb::Status TransLogRecordStore::CheckAndWriteCheckPoint(void) {
            write_lock_.AssertHeld();

            if (writing_checkpoint_) {
                return rocksdb::Status::OK();
            }

            invariant(static_trans_log_current_size_ >= static_trans_log_old_size_);
            // uint64_t check_point_size =
            // BASE_GetParaS64Val(E_SPA_CONFIG_LB_TLOG_CHECK_POINT_BYTES);
            if ((static_trans_log_current_size_ - static_trans_log_old_size_) <
                max_total_trans_log_size_) {
                return rocksdb::Status::OK();
            }

            rocksdb::Status s;
            writing_checkpoint_ = true;
            write_lock_.Unlock();
            s = WriteCheckPoint(false);
            write_lock_.Lock();
            writing_checkpoint_ = false;

            return s;
        }

    }  //  namespace TransactionLog

}  //  namespace mongo
