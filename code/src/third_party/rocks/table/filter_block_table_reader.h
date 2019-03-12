//  Copyright (c) 2017-present.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once
#ifndef ROCKSDB_LITE
#include <string>
#include <memory>
#include <utility>
#include <vector>

#include "db/dbformat.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/table.h"
#include "table/table_reader.h"
#include "table/block_based_table_reader.h"
#include "table/two_level_iterator.h"
#include "util/cf_options.h"
#include "util/file_reader_writer.h"

namespace rocksdb {

class Arena;
class TableReader;
class InternalIterator;

class FilterBlockTableIterator : public TwoLevelIterator {
 public:
  explicit FilterBlockTableIterator(TwoLevelIteratorState* state,
                            InternalIterator* first_level_iter,
                            bool need_free_iter_and_state)
           : TwoLevelIterator(state, first_level_iter, need_free_iter_and_state) {}
  ~FilterBlockTableIterator() {}

  void SetRangeChecker(const IKeyRangeChecker* range_checker, Logger* logger);

  void SkipEmptyDataBlocksForward() override;
  void SkipEmptyDataBlocksBackward() override;
  void SeekForPrev(const Slice& target) override;

 private:
  void SkipNotBelongDataBlocksPre();
  const IKeyRangeChecker* range_checker_ = nullptr;
  Logger* info_log_ = nullptr;
};

class FilterBlockTableReader: public BlockBasedTable {
 public:
  explicit FilterBlockTableReader() : BlockBasedTable() {}
  // Returns a new get over the table contents.
  Status Get(const ReadOptions& read_options, const Slice& key,
             GetContext* get_context, bool skip_filters = false) ;

  // Returns a new iterator over the table contents.
  // The result of NewIterator() is initially invalid (caller must
  // call one of the Seek methods on the iterator before using it).
  // @param skip_filters Disables loading/accessing the filter block
  InternalIterator* NewIterator(const ReadOptions&, Arena* arena = nullptr,
                                bool skip_filters = false) ;

  Status status() const { return status_; }

 private:
  Status status_;
};

extern InternalIterator* NewFilterBlockTableIterator(
    TwoLevelIteratorState* state, InternalIterator* first_level_iter,
    Arena* arena = nullptr, bool need_free_iter_and_state = true);

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
