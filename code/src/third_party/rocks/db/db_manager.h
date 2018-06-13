//  Copyright (c) 2017-present, huawei, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once
#include <string>
#include <vector>

#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/metadata.h"
#include "rocksdb/shared_resource.h"


namespace rocksdb {

class DbManager {
 public:
  DbManager(Env* env, const std::string& p_dbname, const DbInstanceOption& db_options,
    std::shared_ptr<Logger> logger, const std::vector<const IKeyRangeChecker*>& range_checker)
    :env_(env),
     dbname_(p_dbname),
     options_(db_options),
     info_log_(logger),
     checker_list_(range_checker){};
  
  ~DbManager();

  // Prepare the child db files, except the sst file and manifest file
  Status PrepareSplit(std::vector<std::string>& files);

  // Split the DB instance
  Status Split(std::vector<LiveFileMetaData>& metadata, const std::shared_ptr<SharedResource> & shared_checker);

  // split is success, the config confirm this operator.
  Status ConfirmSplit();

  // if the mongodb split fail,need to rollback the split operator.
 Status RollbackSplit();



 private:

  Env* env_;
  const std::string dbname_;
  const DbInstanceOption& options_;
  std::shared_ptr<Logger> info_log_;
  const std::vector<const IKeyRangeChecker*> checker_list_;

};


}// namespace rocksdb
