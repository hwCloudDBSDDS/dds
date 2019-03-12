// Copyright (c) 2017-present. All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once
#ifndef ROCKSDB_LITE

#include <string>
#include "rocksdb/table.h"
#include "rocksdb/options.h"
#include "table/block_based_table_factory.h"

namespace rocksdb {


// FilterBlock Table is which inherits BlockBasedTableFactory, 
// designed for support DB instance split.


class FilterBlockTableFactory : public BlockBasedTableFactory {
 public:
  explicit FilterBlockTableFactory(
    const BlockBasedTableOptions& table_options = BlockBasedTableOptions());
    
  
  ~FilterBlockTableFactory() {}

  const char* Name() const { return "FilterBlockTable"; }

  Status NewTableReader(
      const TableReaderOptions& table_reader_options,
      unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
      unique_ptr<TableReader>* table_reader,
      bool prefetch_index_and_filter_in_cache = true) const ;

  TableBuilder* NewTableBuilder(
      const TableBuilderOptions& table_builder_options,
      uint32_t column_family_id, WritableFileWriter* file) const ;


 private:
  //BlockBasedTableOptions table_options_;
};

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
