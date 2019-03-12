// Copyright (c) 2017-present. All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef ROCKSDB_LITE
#include "table/filter_block_table_factory.h"
#include "table/block_based_table_factory.h"
#include "db/dbformat.h"
#include "table/filter_block_table_builder.h"
#include "table/filter_block_table_reader.h"

namespace rocksdb {

FilterBlockTableFactory::FilterBlockTableFactory(
    const BlockBasedTableOptions& _table_options)
    : BlockBasedTableFactory(_table_options){};

Status FilterBlockTableFactory::NewTableReader(
    const TableReaderOptions& table_reader_options,
    unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
    unique_ptr<TableReader>* table_reader,
    bool prefetch_index_and_filter_in_cache) const {
  return BlockBasedTable::Open(
      table_reader_options.ioptions, table_reader_options.env_options,
      table_options(), table_reader_options.internal_comparator, std::move(file),
      file_size, table_reader, prefetch_index_and_filter_in_cache,
      table_reader_options.skip_filters, table_reader_options.level,
      table_reader_options.do_range_check);
}

TableBuilder* FilterBlockTableFactory::NewTableBuilder(
    const TableBuilderOptions& table_builder_options, uint32_t column_family_id,
    WritableFileWriter* file) const {

  // TODO: change builder to take the option struct
  return new FilterBlockTableBuilder(
     BlockBasedTableFactory::table_options(), table_builder_options,
     column_family_id, file);
}


TableFactory* NewFilterBlockTableFactory(const BlockBasedTableOptions& table_options) {
  return new FilterBlockTableFactory(table_options);
}

}  // namespace rocksdb
#endif  // ROCKSDB_LITE
