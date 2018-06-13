//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
//  Copyright (c) 2018-present, Huawei, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "rocksdb/env.h"
#include "hdfs/env_hdfs.h"
using namespace std;

#ifdef USE_HDFS
#ifndef ROCKSDB_HDFS_FILE_C
#define ROCKSDB_HDFS_FILE_C
#include "rocksdb/options.h"
#include <algorithm>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include <iostream>
#include <sstream>
#include "rocksdb/status.h"
#include "util/string_util.h"
#include <string.h>
#include <mutex>
#include "util/coding.h"

#define HDFS_EXISTS 0
#define HDFS_DOESNT_EXIST -1
#define HDFS_SUCCESS 0

//
// This file defines an HDFS environment for rocksdb. It uses the libhdfs
// api to access HDFS. All HDFS files created by one instance of rocksdb
// will reside on the same HDFS cluster.
//

namespace rocksdb {

namespace {

// Log error message
static Status IOError(const std::string& context, int err_number) {
  return (err_number == ENOSPC) ?
      Status::NoSpace(context, strerror(err_number)) :
      Status::IOError(context, strerror(err_number));
}

// assume that there is one global logger for now. It is not thread-safe,
// but need not be because the logger is initialized at db-open time.
static Logger* mylog = nullptr;

// Used for reading a file from HDFS. It implements both sequential-read
// access methods as well as random read access methods.
class HdfsReadableFile : virtual public SequentialFile,
                         virtual public RandomAccessFile {
 private:
  hdfsFS fileSys_;
  std::string filename_;
  hdfsFile hfile_;

 public:
  HdfsReadableFile(hdfsFS fileSys, const std::string& fname)
      : fileSys_(fileSys), filename_(fname), hfile_(nullptr) {
    Log(InfoLogLevel::DEBUG_LEVEL, mylog,
        "[hdfs] HdfsReadableFile opening file %s\n",
        filename_.c_str());
    hfile_ = hdfsOpenFile(fileSys_, filename_.c_str(), O_RDONLY, 0, 0, 0);
    Log(InfoLogLevel::DEBUG_LEVEL, mylog,
        "[hdfs] HdfsReadableFile opened file %s hfile_=0x%p\n",
        filename_.c_str(), hfile_);
  }

  virtual ~HdfsReadableFile() {
    Log(InfoLogLevel::DEBUG_LEVEL, mylog,
        "[hdfs] HdfsReadableFile closing file %s\n",
        filename_.c_str());
    hdfsCloseFile(fileSys_, hfile_);
    Log(InfoLogLevel::DEBUG_LEVEL, mylog,
        "[hdfs] HdfsReadableFile closed file %s\n",
        filename_.c_str());
    hfile_ = nullptr;
  }

  bool isValid() {
    return hfile_ != nullptr;
  }

  // sequential access, read data at current offset in file
  virtual Status Read(size_t n, Slice* result, char* scratch) {
    Status s;
    Log(InfoLogLevel::DEBUG_LEVEL, mylog,
        "[hdfs] HdfsReadableFile reading %s %ld\n",
        filename_.c_str(), n);

    char* buffer = scratch;
    size_t total_bytes_read = 0;
    tSize bytes_read = 0;
    tSize remaining_bytes = (tSize)n;

    // Read a total of n bytes repeatedly until we hit error or eof
    while (remaining_bytes > 0) {
      bytes_read = hdfsRead(fileSys_, hfile_, buffer, remaining_bytes);
      if (bytes_read <= 0) {
        break;
      }
      assert(bytes_read <= remaining_bytes);

      total_bytes_read += bytes_read;
      remaining_bytes -= bytes_read;
      buffer += bytes_read;
    }
    assert(total_bytes_read <= n);

    Log(InfoLogLevel::DEBUG_LEVEL, mylog,
        "[hdfs] HdfsReadableFile read %s\n", filename_.c_str());

    if (bytes_read < 0) {
      s = IOError(filename_, errno);
    } else {
      *result = Slice(scratch, total_bytes_read);
    }

    return s;
  }

  // random access, read data from specified offset in file
  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const {
    Status s;
    Log(InfoLogLevel::DEBUG_LEVEL, mylog,
        "[hdfs] HdfsReadableFile preading %s\n", filename_.c_str());
    ssize_t bytes_read = hdfsPread(fileSys_, hfile_, offset,
                                   (void*)scratch, (tSize)n);
    Log(InfoLogLevel::DEBUG_LEVEL, mylog,
        "[hdfs] HdfsReadableFile pread %s\n", filename_.c_str());
    *result = Slice(scratch, (bytes_read < 0) ? 0 : bytes_read);
    if (bytes_read < 0) {
      // An error: return a non-ok status
      s = IOError(filename_, errno);
    }
    return s;
  }

  virtual Status Skip(uint64_t n) {
    Log(InfoLogLevel::DEBUG_LEVEL, mylog,
        "[hdfs] HdfsReadableFile skip %s\n", filename_.c_str());
    // get current offset from file
    tOffset current = hdfsTell(fileSys_, hfile_);
    if (current < 0) {
      return IOError(filename_, errno);
    }
    // seek to new offset in file
    tOffset newoffset = current + n;
    int val = hdfsSeek(fileSys_, hfile_, newoffset);
    if (val < 0) {
      return IOError(filename_, errno);
    }
    return Status::OK();
  }

 private:

  // returns true if we are at the end of file, false otherwise
  bool feof() {
    Log(InfoLogLevel::DEBUG_LEVEL, mylog,
        "[hdfs] HdfsReadableFile feof %s\n", filename_.c_str());
    if (hdfsTell(fileSys_, hfile_) == fileSize()) {
      return true;
    }
    return false;
  }

  // the current size of the file
  tOffset fileSize() {
    Log(InfoLogLevel::DEBUG_LEVEL, mylog,
        "[hdfs] HdfsReadableFile fileSize %s\n", filename_.c_str());
    hdfsFileInfo* pFileInfo = hdfsGetPathInfo(fileSys_, filename_.c_str());
    tOffset size = 0L;
    if (pFileInfo != nullptr) {
      size = pFileInfo->mSize;
      hdfsFreeFileInfo(pFileInfo, 1);
    } else {
      throw HdfsFatalException("fileSize on unknown file " + filename_);
    }
    return size;
  }
};

// Appends to an existing file in HDFS.
class HdfsWritableFile: public WritableFile {
 private:
  hdfsFS fileSys_;
  std::string filename_;
  hdfsFile hfile_;

 public:
  HdfsWritableFile(hdfsFS fileSys, const std::string& fname)
      : fileSys_(fileSys), filename_(fname) , hfile_(nullptr) {
    Log(InfoLogLevel::DEBUG_LEVEL, mylog,
        "[hdfs] HdfsWritableFile opening %s\n", filename_.c_str());
    hfile_ = hdfsOpenFile(fileSys_, filename_.c_str(), O_WRONLY, 0, 0, 0);
    Log(InfoLogLevel::DEBUG_LEVEL, mylog,
        "[hdfs] HdfsWritableFile opened %s\n", filename_.c_str());
    assert(hfile_ != nullptr);
  }
  virtual ~HdfsWritableFile() {
    if (hfile_ != nullptr) {
      Log(InfoLogLevel::DEBUG_LEVEL, mylog,
          "[hdfs] HdfsWritableFile closing %s\n", filename_.c_str());
      //hdfsCloseFile(fileSys_, hfile_);
      Log(InfoLogLevel::DEBUG_LEVEL, mylog,
          "[hdfs] HdfsWritableFile closed %s\n", filename_.c_str());
      hfile_ = nullptr;
    }
  }
  bool IsSyncThreadSafe() const { return true; }
  // If the file was successfully created, then this returns true.
  // Otherwise returns false.
  bool isValid() {
    return hfile_ != nullptr;
  }

  // The name of the file, mostly needed for debug logging.
  const std::string& getName() {
    return filename_;
  }

  virtual Status Append(const Slice& data) {
    Log(InfoLogLevel::DEBUG_LEVEL, mylog,
        "[hdfs] HdfsWritableFile Append %s\n", filename_.c_str());
    const char* src = data.data();
    size_t left = data.size();
    size_t ret = hdfsWrite(fileSys_, hfile_, src, left);
    Log(InfoLogLevel::DEBUG_LEVEL, mylog,
        "[hdfs] HdfsWritableFile Appended %s\n", filename_.c_str());
    if (ret != left) {
      return IOError(filename_, errno);
    }
    return Status::OK();
  }

  virtual Status Flush() {
    return Sync();
  }

  virtual Status Sync() {
    Status s;
    Log(InfoLogLevel::DEBUG_LEVEL, mylog,
        "[hdfs] HdfsWritableFile Sync %s\n", filename_.c_str());
    if (hdfsFlush(fileSys_, hfile_) == -1) {
      return IOError(filename_, errno);
    }
    if (hdfsHSync(fileSys_, hfile_) == -1) {
      return IOError(filename_, errno);
    }
    Log(InfoLogLevel::DEBUG_LEVEL, mylog,
        "[hdfs] HdfsWritableFile Synced %s\n", filename_.c_str());
    return Status::OK();
  }

  // This is used by HdfsLogger to write data to the debug log file
  virtual Status Append(const char* src, size_t size) {
    if (hdfsWrite(fileSys_, hfile_, src, size) != (tSize)size) {
      return IOError(filename_, errno);
    }
    return Status::OK();
  }

  virtual Status Close() {
    Log(InfoLogLevel::DEBUG_LEVEL, mylog,
        "[hdfs] HdfsWritableFile closing %s\n", filename_.c_str());
    if (hdfsCloseFile(fileSys_, hfile_) != 0) {
      return IOError(filename_, errno);
    }
    Log(InfoLogLevel::DEBUG_LEVEL, mylog,
        "[hdfs] HdfsWritableFile closed %s\n", filename_.c_str());
    hfile_ = nullptr;
    return Status::OK();
  }
};

// The object that implements the debug logs to reside in HDFS.
class HdfsLogger : public Logger {
 private:
  HdfsWritableFile* file_;
  uint64_t (*gettid_)();  // Return the thread id for the current thread

 public:
  HdfsLogger(HdfsWritableFile* f, uint64_t (*gettid)())
      : file_(f), gettid_(gettid) {
    Log(InfoLogLevel::DEBUG_LEVEL, mylog,
        "[hdfs] HdfsLogger opened %s\n",
        file_->getName().c_str());
  }

  virtual ~HdfsLogger() {
    Log(InfoLogLevel::DEBUG_LEVEL, mylog,
        "[hdfs] HdfsLogger closed %s\n",
        file_->getName().c_str());
    delete file_;
    if (mylog != nullptr && mylog == this) {
      mylog = nullptr;
    }
  }

  virtual void Logv(const InfoLogLevel log_level, const char* format, va_list ap)
  {
     return Logger::Logv(log_level, format, ap);
  }
  
  virtual void Logv(const char* format, va_list ap) {
    const uint64_t thread_id = (*gettid_)();

    // We try twice: the first time with a fixed-size stack allocated buffer,
    // and the second time with a much larger dynamically allocated buffer.
    char buffer[500];
    for (int iter = 0; iter < 2; iter++) {
      char* base;
      int bufsize;
      if (0 == iter) {
        bufsize = sizeof(buffer);
        base = buffer;
      } else {
        bufsize = 30000;
        base = new char[bufsize];
      }
      char* p = base;
      char* limit = base + bufsize;

      struct timeval now_tv;
      gettimeofday(&now_tv, nullptr);
      const time_t seconds = now_tv.tv_sec;
      struct tm t;
      localtime_r(&seconds, &t);
      p += snprintf(p, limit - p,
                    "%04d/%02d/%02d-%02d:%02d:%02d.%06d %llx ",
                    t.tm_year + 1900,
                    t.tm_mon + 1,
                    t.tm_mday,
                    t.tm_hour,
                    t.tm_min,
                    t.tm_sec,
                    static_cast<int>(now_tv.tv_usec),
                    static_cast<long long unsigned int>(thread_id));

      // Print the message
      if (p < limit) {
        va_list backup_ap;
        va_copy(backup_ap, ap);
        p += vsnprintf(p, limit - p, format, backup_ap);
        va_end(backup_ap);
      }

      // Truncate to available space if necessary
      if (p >= limit) {
        if (0 == iter) {
          continue;       // Try again with larger buffer
        } else {
          p = limit - 1;
        }
      }

      // Add newline if necessary
      if (p == base || '\n' != p[-1]) {
        *p++ = '\n';
      }

      assert(p <= limit);
      file_->Append(base, p-base);
      file_->Flush();
      if (base != buffer) {
        delete[] base;
      }
      break;
    }
  }
};

}  // namespace

// Finally, the hdfs environment

void HdfsEnv::CheckFilePath(const std::string& old_fname, std::string& new_fname){

  if((nullptr == shared_checker_)  
    || (strstr(old_fname.c_str(), ".sst") == nullptr)){
    
    Log(InfoLogLevel::DEBUG_LEVEL, info_log_, 
                        "HDFS Env no need check the file(%s), shared_checker(%p)", 
                        old_fname.c_str(), shared_checker_.get());
    return ;
  }

  std::string id;
  id.assign(old_fname, old_fname.rfind("/")+1, old_fname.size()-old_fname.rfind("/")-1);
  
  
  bool s = shared_checker_->CheckSharedResource(id, new_fname);
  if (true == s){
    Log(InfoLogLevel::DEBUG_LEVEL, info_log_, 
                        "HDFS Env check (%p) the resourceid %s, old_file(%s), new_file(%s) is shared", 
                        shared_checker_.get(), id.c_str(), old_fname.c_str(), new_fname.c_str());
  }else{
  Log(InfoLogLevel::INFO_LEVEL, info_log_, 
                      "HDFS Env check (%p) the resourceid %s, old_file(%s), new_file(%s) is no shared", 
                      shared_checker_.get(), id.c_str(), old_fname.c_str(), new_fname.c_str());
  }
  return;
}
const std::string HdfsEnv::kProto = "hdfs://";
const std::string HdfsEnv::pathsep = "/";

// open a file for sequential reading
Status HdfsEnv::NewSequentialFile(const std::string& fname1,
                                  unique_ptr<SequentialFile>* result,
                                  const EnvOptions& options) {
  result->reset();
  std::string fname = fname1;
  CheckFilePath(fname1, fname);
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, 
                      "HDFS Env NewSequentialFile old file(%s)new(%s) Env(%p)",
                      fname1.c_str(), fname.c_str(),this);
  if(use_posix_){
    return posixEnv->NewSequentialFile(fname, result,options);
  }
  HdfsReadableFile* f = new HdfsReadableFile(fileSys_, fname);
  if (f == nullptr || !f->isValid()) {
    delete f;
    *result = nullptr;
    return IOError(fname, errno);
  }
  result->reset(dynamic_cast<SequentialFile*>(f));
  return Status::OK();
}

// open a file for random reading
Status HdfsEnv::NewRandomAccessFile(const std::string& fname1,
                                    unique_ptr<RandomAccessFile>* result,
                                    const EnvOptions& options) {
  result->reset();
  std::string fname = fname1;
  CheckFilePath(fname1, fname);
  
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, 
                      "HDFS Env NewRandomAccessFile old file(%s) new(%s) Env(%p)",
                      fname1.c_str(), fname.c_str(),this);
  if(use_posix_){
    return posixEnv->NewRandomAccessFile(fname, result,options);
  }
  HdfsReadableFile* f = new HdfsReadableFile(fileSys_, fname);
  if (f == nullptr || !f->isValid()) {
    delete f;
    *result = nullptr;
    return IOError(fname, errno);
  }
  result->reset(dynamic_cast<RandomAccessFile*>(f));
  return Status::OK();
}

// create a new file for writing
Status HdfsEnv::NewWritableFile(const std::string& fname,
                                unique_ptr<WritableFile>* result,
                                const EnvOptions& options) {
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, 
                      "HDFS Env NewWritableFile file(%s) Env(%p)",fname.c_str(), this);

  if(use_posix_){
    return posixEnv->NewWritableFile(fname, result,options);
  }
  result->reset();
  Status s;
  HdfsWritableFile* f = new HdfsWritableFile(fileSys_, fname);
  if (f == nullptr || !f->isValid()) {
    delete f;
    *result = nullptr;
    return IOError(fname, errno);
  }
  result->reset(dynamic_cast<WritableFile*>(f));
  return Status::OK();
}

class HdfsDirectory : public Directory {
 public:
  explicit HdfsDirectory(int fd) : fd_(fd) {}
  ~HdfsDirectory() {}

  virtual Status Fsync() { return Status::OK(); }

 private:
  int fd_;
};

Status HdfsEnv::NewDirectory(const std::string& name,
                             unique_ptr<Directory>* result) {
  if(use_posix_){
    return posixEnv->NewDirectory(name, result);
  }
  
  int value = hdfsExists(fileSys_, name.c_str());
  switch (value) {
    case HDFS_EXISTS:
      result->reset(new HdfsDirectory(0));
      return Status::OK();
    default:  // fail if the directory doesn't exist
      Log(InfoLogLevel::FATAL_LEVEL,
          mylog, "NewDirectory hdfsExists call failed");
      throw HdfsFatalException("hdfsExists call failed with error " +
                               ToString(value) + " on path " + name +
                               ".\n");
  }
}

Status HdfsEnv::FileExists(const std::string& fname) {

  
  if(use_posix_){
    return posixEnv->FileExists(fname);
  }
  
  int value = hdfsExists(fileSys_, fname.c_str());
  switch (value) {
    case HDFS_EXISTS:
      return Status::OK();
    case HDFS_DOESNT_EXIST:
      return Status::NotFound();
    default:  // anything else should be an error
      Log(InfoLogLevel::FATAL_LEVEL,
          mylog, "FileExists hdfsExists call failed");
      return Status::IOError("hdfsExists call failed with error " +
                             ToString(value) + " on path " + fname + ".\n");
  }
}

Status HdfsEnv::GetChildren(const std::string& path,
                            std::vector<std::string>* result) {
  if(use_posix_){
    return posixEnv->GetChildren(path, result);
  }
  
  result->clear();
  int value = hdfsExists(fileSys_, path.c_str());
  switch (value) {
    case HDFS_EXISTS: {  // directory exists
    int numEntries = 0;
    hdfsFileInfo* pHdfsFileInfo = 0;
    pHdfsFileInfo = hdfsListDirectory(fileSys_, path.c_str(), &numEntries);
    if (numEntries >= 0) {
      for(int i = 0; i < numEntries; i++) {
        char* pathname = pHdfsFileInfo[i].mName;
        char* filename = rindex(pathname, '/');
        if (filename != nullptr) {
          result->push_back(filename+1);
        }
      }
      if (pHdfsFileInfo != nullptr) {
        hdfsFreeFileInfo(pHdfsFileInfo, numEntries);
      }
    } else {
      // numEntries < 0 indicates error
      Log(InfoLogLevel::FATAL_LEVEL, mylog,
          "hdfsListDirectory call failed with error ");
      throw HdfsFatalException(
          "hdfsListDirectory call failed negative error.\n");
    }
    break;
  }
  case HDFS_DOESNT_EXIST:  // directory does not exist, exit
    return Status::NotFound();
  default:          // anything else should be an error
    Log(InfoLogLevel::FATAL_LEVEL, mylog,
        "GetChildren hdfsExists call failed");
    throw HdfsFatalException("hdfsExists call failed with error " +
                             ToString(value) + ".\n");
  }
  return Status::OK();
}

Status HdfsEnv::DeleteFile(const std::string& fname) {
 
  if(shared_checker_ && (strstr(fname.c_str(), ".sst") != nullptr)){
    SharedResourceRemoveDescription resource;
    std::vector<SharedResourceRemoveDescription> list;
    std::string id;
    list.clear();
    id.assign(fname, fname.rfind("/")+1, fname.size()-fname.rfind("/")-1);
    resource.id = id;
    resource.remove_result = 0;
    resource.shared_flag = false;
    list.push_back(resource);
    Log(InfoLogLevel::DEBUG_LEVEL, info_log_, 
                        "HDFS Env DeleteFile file(%s) to RemoveSharedResource id(%s),checker(%p)",
                        fname.c_str(), id.c_str(), shared_checker_.get());
    for(int i = 0; i < 5; i++){      
      shared_checker_->RemoveSharedResource(list);
      if(list[0].remove_result == 0){
        if(list[0].shared_flag == true){
          return Status::OK();
        }else{
          break;
        }
      }
    }
    if(list[0].remove_result != 0){
      Log(InfoLogLevel::ERROR_LEVEL, info_log_, 
                          "HDFS Env DeleteFile file(%s) RemoveSharedResource failed(%d) id(%s)",
                          fname.c_str(), list[0].remove_result, id.c_str());
     
      return Status::IOError("DeleteFile RemoveSharedResource error id(" + id +
                             ")  file `" + fname + "' ");
    }
  }
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, 
                      "HDFS Env DeleteFile file(%s) Env(%p)",fname.c_str(), this);
  if(use_posix_){
    
    return posixEnv->DeleteFile(fname);
  }
  
  if (hdfsDelete(fileSys_, fname.c_str(), 1) == 0) {
    return Status::OK();
  }
  return IOError(fname, errno);
};

Status HdfsEnv::CreateDir(const std::string& name) {
  
  if(use_posix_){
    return posixEnv->CreateDir(name);
  }
  
  if (hdfsCreateDirectory(fileSys_, name.c_str()) == 0) {
    return Status::OK();
  }
  Log(InfoLogLevel::ERROR_LEVEL, info_log_, 
      "HDFS Env CreateDir DIR(%s) errno(%d) Env(%p)",name.c_str(), errno, this);
  return IOError(name, errno);
};

Status HdfsEnv::CreateDirIfMissing(const std::string& name) {
  if(use_posix_){
    std::string filepath = name;
    size_t pos = 1;

    while(true) {
        if (pos == std::string::npos) {
            filepath = name;
            pos = name.size();
        } else {
            filepath.assign(name, 0, name.find("/", pos));
        }

        auto s = FileExists(filepath);
        if (s == rocksdb::Status::NotFound()) {
            s = CreateDir(filepath);
            if (!s.ok()) {
                s = FileExists(filepath);
                if (!s.ok()) {
                    Log(InfoLogLevel::ERROR_LEVEL, info_log_, 
                        "HDFS Env CreateDirIfMissing DIR(%s) pathDir(%s), RET(%s) Env(%p)",name.c_str(),
                        filepath.c_str(), s.ToString().c_str(), this);
                    return s;
                }
            }
        } else if (!s.ok()) {
            Log(InfoLogLevel::ERROR_LEVEL, info_log_, 
                "HDFS Env CreateDirIfMissing DIR(%s) pathDir(%s) RET(%s) Env(%p)", name.c_str(),
                filepath.c_str(), s.ToString().c_str(), this);
            return s;
        } 

        pos += 1;
        if (pos >= name.length()) {
            break;
        }

        pos = name.find("/", pos);
    } 

    return rocksdb::Status::OK();
  }
  
  const int value = hdfsExists(fileSys_, name.c_str());
  //  Not atomic. state might change b/w hdfsExists and CreateDir.
  switch (value) {
    case HDFS_EXISTS:
    return Status::OK();
    case HDFS_DOESNT_EXIST:
    return CreateDir(name);
    default:  // anything else should be an error
      Log(InfoLogLevel::FATAL_LEVEL, mylog,
          "CreateDirIfMissing hdfsExists call failed");
      throw HdfsFatalException("hdfsExists call failed with error " +
                               ToString(value) + ".\n");
  }
};

Status HdfsEnv::DeleteDir(const std::string& name) {
  
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, 
                      "HDFS Env DeleteDir DIR(%s) Env(%p)",name.c_str(), this);
  if(use_posix_){
    
    Status result;
    char cmd[512] = {0};
    sprintf(cmd,"rm -rf %s", name.c_str());
    
    int ret = system(cmd);
    if(-1 == ret){
      Log(InfoLogLevel::ERROR_LEVEL, info_log_, 
          "HDFS Env DeleteDir DIR(%s) RET(%d) Env(%p)",name.c_str(), ret, this);
      return result = Status::IOError("system mkdir `"+name+"' system error");
    }else{
      if (WIFEXITED(ret) && (0 == WEXITSTATUS(ret))){
        return Status::OK();
      }
      Log(InfoLogLevel::ERROR_LEVEL, info_log_, 
      "HDFS Env DeleteDir WIFEXITED(%d) WEXITSTATUS(%d) DIR(%s) RET(%d) Env(%p)",
      WIFEXITED(ret), WEXITSTATUS(ret), name.c_str(), ret, this);
      return result = Status::IOError("system shell fail");
    }
  }

  Status ret = DeleteFile(name);;
  if (!ret.ok()) {
    Log(InfoLogLevel::ERROR_LEVEL, info_log_, 
        "HDFS Env DeleteDir DIR(%s) errno(%d) Env(%p)",name.c_str(), errno, this);
    return Status::IOError("delete file fail");
  }
  
  return Status::OK();
};

Status HdfsEnv::GetFileSize(const std::string& fname1, uint64_t* size) {
  std::string fname = fname1;
  CheckFilePath(fname1, fname);
  Log(InfoLogLevel::DEBUG_LEVEL, info_log_, 
                      "Stream Env GetFileSize old file(%s) new file(%s) Env(%p)",
                      fname1.c_str(), fname.c_str(), this);
  if(use_posix_){
    
    return posixEnv->GetFileSize(fname, size);
  }
  
  *size = 0L;
  hdfsFileInfo* pFileInfo = hdfsGetPathInfo(fileSys_, fname.c_str());
  if (pFileInfo != nullptr) {
    *size = pFileInfo->mSize;
    hdfsFreeFileInfo(pFileInfo, 1);
    return Status::OK();
  }
  return IOError(fname, errno);
}

Status HdfsEnv::GetFileModificationTime(const std::string& fname,
                                        uint64_t* time) {
  if(use_posix_){
    return posixEnv->GetFileModificationTime(fname, time);
  }
  hdfsFileInfo* pFileInfo = hdfsGetPathInfo(fileSys_, fname.c_str());
  if (pFileInfo != nullptr) {
    *time = static_cast<uint64_t>(pFileInfo->mLastMod);
    hdfsFreeFileInfo(pFileInfo, 1);
    return Status::OK();
  }
  return IOError(fname, errno);

}

// The rename is not atomic. HDFS does not allow a renaming if the
// target already exists. So, we delete the target before attempting the
// rename.
Status HdfsEnv::RenameFile(const std::string& src, const std::string& target) {
  if(use_posix_){
    return posixEnv->RenameFile(src, target);
  }
  
  hdfsDelete(fileSys_, target.c_str(), 1);
  if (hdfsRename(fileSys_, src.c_str(), target.c_str()) == 0) {
    return Status::OK();
  }
  return IOError(src, errno);
}

Status HdfsEnv::LockFile(const std::string& fname, FileLock** lock) {
  if(use_posix_){
    return posixEnv->LockFile(fname, lock);
  }
  // there isn's a very good way to atomically check and create
  // a file via libhdfs
  *lock = nullptr;
  return Status::OK();
}

Status HdfsEnv::UnlockFile(FileLock* lock) {
  if(use_posix_){
    return posixEnv->UnlockFile(lock);
  }
  return Status::OK();
}

Status HdfsEnv::NewLogger(const std::string& fname,
                          shared_ptr<Logger>* result) {
  if(use_posix_){
    return posixEnv->NewLogger(fname, result);
  }
  HdfsWritableFile* f = new HdfsWritableFile(fileSys_, fname);
  if (f == nullptr || !f->isValid()) {
    delete f;
    *result = nullptr;
    return IOError(fname, errno);
  }
  HdfsLogger* h = new HdfsLogger(f, &HdfsEnv::gettid);
  result->reset(h);
  if (mylog == nullptr) {
    // mylog = h; // uncomment this for detailed logging
  }
  return Status::OK();
}

// The factory method for creating an HDFS Env
Status NewHdfsEnv(Env** hdfs_env, const std::string& fsname, bool use_posix) {
  *hdfs_env = new HdfsEnv(fsname, use_posix);
  return Status::OK();
}
}  // namespace rocksdb

#endif // ROCKSDB_HDFS_FILE_C

#else // USE_HDFS

// dummy placeholders used when HDFS is not available
namespace rocksdb {
 Status HdfsEnv::NewSequentialFile(const std::string& fname,
                                   unique_ptr<SequentialFile>* result,
                                   const EnvOptions& options) {
   return Status::NotSupported("Not compiled with hdfs support");
 }

 Status NewHdfsEnv(Env** hdfs_env, const std::string& fsname, bool use_posix) {
   return Status::NotSupported("Not compiled with hdfs support");
 }
}

#endif
