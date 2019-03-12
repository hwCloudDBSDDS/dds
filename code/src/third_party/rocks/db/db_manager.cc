//  Copyright (c) 2017-present.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "db/db_manager.h"
#include "db/filename.h"
#include "util/file_util.h"

#define RET_OK   (0)

namespace rocksdb {

Status DbManager::PrepareSplit(std::vector<std::string>& files){
  Status s;

  //check path info
  s = env_->FileExists(dbname_);
  if (!s.ok()) {
    return Status::InvalidArgument("Parent DB Directory not exists");
  } 

  s = env_->FileExists(options_.instance_path);
  if (!s.IsNotFound()) {
    return Status::InvalidArgument("Child DB Directory exists");
  } 

  s = env_->CreateDir(options_.instance_path);
  if (!s.ok()) {
    return Status::InvalidArgument("Create Child DB Directory fail");
  } 
  
  // copy files
  for (size_t i = 0; s.ok() && i < files.size(); ++i) {
    uint64_t number;
    FileType type;
    bool ok = ParseFileName(files[i], &number, &type);
    if (!ok) {
      s = Status::Corruption("Can't parse file name. This is very bad in split");
      break;
    }
    // we should only get options,lock and current files here
    assert(type == kDBLockFile || type == kOptionsFile);
    assert(files[i].size() > 0 && files[i][0] == '/');
    
    std::string src_fname = files[i];
    Log(InfoLogLevel::INFO_LEVEL, info_log_, "Split DB Copying: %s,parent:%s, child:%s ", 
      src_fname.c_str(), dbname_.c_str(), options_.instance_path.c_str());
    s = CopyFile(env_, dbname_+ src_fname,
                 options_.instance_path + src_fname, 0, false);
    if(!s.ok()){
        break;
      }
  }

  if (!s.ok()) {
    // clean all the files we might have created
    Log(InfoLogLevel::WARN_LEVEL, info_log_, "Copy file Snapshot failed -- %s", s.ToString().c_str());
    // we have to delete the dir and all its children
    std::vector<std::string> subchildren;
    env_->GetChildren(options_.instance_path, &subchildren);
    for (auto& subchild : subchildren) {
      std::string subchild_path = options_.instance_path + "/" + subchild;
      Status s1 = env_->DeleteFile(subchild_path);
      Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "Delete Child DB file %s -- %s", subchild_path.c_str(),
          s1.ToString().c_str());
    }
    // finally delete the private dir
    Status s1 = env_->DeleteDir(options_.instance_path);
    Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "Delete Child DB dir %s -- %s", options_.instance_path.c_str(),
        s1.ToString().c_str());
    return s;
  }

  Log(InfoLogLevel::INFO_LEVEL, info_log_, "Prepare child db files success from PrepareSplit");
  
  return Status::OK();
}


 // Split the DB instance
Status DbManager::Split(std::vector<LiveFileMetaData>& metadata, 
                        const std::shared_ptr<SharedResource>& shared_checker){

  const IKeyRangeChecker* range_checker = nullptr;
  std::vector<SharedResourceId> file_list;
  
  file_list.clear();
  
  for (const auto& file : metadata) {
    uint64_t number;
    FileType type;
    if (!ParseFileName(file.name, &number, "LOG", &type)) {
      continue;
    }
    
    if (type == kTableFile) {
      //get checker
      if(checker_list_.size() > 0) {
        for (auto iter = checker_list_.begin(); iter != checker_list_.end(); ++iter){
          if(0 == strcmp((*iter)->GetName(), file.column_family_name.c_str())){
              range_checker = (*iter);
              break;
          }
        }
      }
      
      //check the file is need to write to transaction log
    
      if(nullptr != range_checker &&
         KeyRangeCheckResult::RangeSeparated
             == range_checker->DoesKeyRangeBelongToChunk(file.smallestkey,
                                                   file.largestkey)){
         Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
           "cfd[%s]:file(%s) path(%s)not write transaction log",
           file.column_family_name.c_str(), file.name.c_str(), file.db_path.c_str());
         continue;
      }
      
      Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
        "cfd[%s]:file(%s) path(%s)need write transaction log",
        file.column_family_name.c_str(), file.name.c_str(), file.db_path.c_str());
      
      std::string id;
      id.assign(file.name, file.name.rfind("/")+1, file.name.size()-file.name.rfind("/")-1);
      file_list.push_back(id);
    }
    else{
      Log(InfoLogLevel::WARN_LEVEL, info_log_, "Add ref file find not sst file file(%llu),type(%d),cf(%s) ",
        number, type, file.column_family_name.c_str());
      }
  }

   //add the reference to tlog
  if(nullptr != shared_checker){
   int s = shared_checker->RegisterSharedResource(file_list);
  
    if(s != RET_OK){
      Log(InfoLogLevel::ERROR_LEVEL, info_log_, "Add ref file list fail count(%d)",file_list.size());
      return Status::InvalidArgument("Add ref file failed");
    }
  }
    
  return Status::OK();
}

 // split is success, the config confirm this operator.
Status DbManager::ConfirmSplit(){

  return Status::OK();
}

 // if the mongodb split fail,need to rollback the split operator.
Status DbManager::RollbackSplit(){

   Status s;

   // clean all the files we might have created
   Log(InfoLogLevel::WARN_LEVEL, info_log_, "Split Rollback the files child(%s) parent(%s)", 
   options_.instance_path.c_str(), dbname_.c_str());
   // we have to delete the dir and all its children
   std::vector<std::string> subdir;
   env_->GetChildren(options_.instance_path, &subdir);
   for (auto& subchild : subdir) {
     std::string subchild_path = options_.instance_path + "/" + subchild;
     s = env_->DeleteFile(subchild_path);
     Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "Delete Child DB file %s -- result%s", subchild_path.c_str(),
         s.ToString().c_str());
   }
   // finally delete the private dir
   s = env_->DeleteDir(options_.instance_path);
   Log(InfoLogLevel::DEBUG_LEVEL, info_log_, "Delete Child DB dir %s -- %s", options_.instance_path.c_str(),
       s.ToString().c_str());
   if(!s.ok()){
     Log(InfoLogLevel::WARN_LEVEL, info_log_, "Rollback split delete files failed %s", s.ToString().c_str());
     return s;
   }
   
  return Status::OK();
}


}// namespace rocksdb 
