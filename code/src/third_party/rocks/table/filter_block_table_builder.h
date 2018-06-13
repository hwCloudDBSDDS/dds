//  Copyright (c) 2017-present, Huawei, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once
#ifndef ROCKSDB_LITE
#include <stdint.h>
#include <limits>
#include <string>
#include <utility>
#include <vector>
#include "port/port.h"
#include "rocksdb/status.h"
#include "table/block_based_table_builder.h"
#include "rocksdb/table.h"
#include "rocksdb/table_properties.h"

namespace rocksdb {

class FilterBlockTableBuilder: public BlockBasedTableBuilder {
 public:
     // Create a builder that will store the contents of the table it is
     // building in *file.  Does not close the file.  It is up to the
     // caller to close the file after calling Finish().
  FilterBlockTableBuilder(
      const BlockBasedTableOptions& table_options,
      const TableBuilderOptions& table_builder_options,
      uint32_t column_family_id,
      WritableFileWriter* file)
       : BlockBasedTableBuilder(
      table_builder_options.ioptions, table_options,
      table_builder_options.internal_comparator,
      table_builder_options.int_tbl_prop_collector_factories, column_family_id,
      file, table_builder_options.compression_type,
      table_builder_options.compression_opts,
      table_builder_options.compression_dict,
      table_builder_options.skip_filters,
      table_builder_options.column_family_name){};

  // REQUIRES: Either Finish() or Abandon() has been called.
  ~FilterBlockTableBuilder() {}

 const char* Name() const  { return "FilterBlockTableBuilder"; }

 private:

  // No copying allowed
  FilterBlockTableBuilder(const FilterBlockTableBuilder&) = delete;
  void operator=(const FilterBlockTableBuilder&) = delete;
};

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
