//  Copyright (c) 2017-present.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef ROCKSDB_LITE
#include <algorithm>
#include <limits>
#include <string>
#include <utility>
#include <vector>
#include "rocksdb/iterator.h"
#include "rocksdb/table.h"
#include "table/internal_iterator.h"
#include "table/meta_blocks.h"
#include "table/filter_block_table_reader.h"
#include "table/filter_block_table_factory.h"
#include "table/get_context.h"
#include "util/arena.h"
#include "util/coding.h"

namespace rocksdb {

void FilterBlockTableIterator::SetRangeChecker(const IKeyRangeChecker* range_checker, Logger* logger){
  range_checker_ = range_checker;
  info_log_ = logger;
  return;
}

void FilterBlockTableIterator::SkipNotBelongDataBlocksPre(){
  while (Valid()) {
    if (range_checker_ == nullptr){
      return ;
    }
    else{
      Slice current_value = value();
      Slice current_key = key();

      ParsedInternalKey ikey;
      if(ParseInternalKey(current_key, &ikey)){
        if(!IsNeedCheckValueType(ikey.type)){
          Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
                "FilterChecker skip pre key: type(%d)size(%d) value: (%d)", 
                ikey.type, ikey.user_key.size(), current_value.size());
          return;
        }else if(range_checker_->DoesRecordBelongToChunk(ikey.user_key, current_value)){
        //Range_checker is not appointed or the current KV matches to it.
        return;
        }
      Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
              "FilterChecker skip pre key key type(%d)size(%d) value: (%d)", 
              ikey.type, ikey.user_key.size(), current_value.size());
      }
    }

    //The current KV doesn't match to range_checker, move to previous KV.
    second_level_iter_.Prev();
    continue;
  }
}

//"Empty" means that no KV matches to range_checker.
void FilterBlockTableIterator::SkipEmptyDataBlocksForward() {
  while (true) {
    if (Valid()){
      if (range_checker_ == nullptr){
        return ;
      }
      else{
        Slice cur_key = key();
        Slice cur_value = value();

        ParsedInternalKey ikey;
        if(ParseInternalKey(cur_key, &ikey)){
          if(!IsNeedCheckValueType(ikey.type)){
            if(ikey.type != kTypeDeletion){
            Log(InfoLogLevel::WARN_LEVEL, info_log_,
                  "SST FilterChecker skip forward key: type(%d)size(%d)value: (%d)", 
                  ikey.type, ikey.user_key.size(), cur_value.size());
              }
            return;
          }else if(range_checker_->DoesRecordBelongToChunk(ikey.user_key, cur_value)){
          //Range_checker is not appointed or the current KV matches to it.
          return;
          }
        Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
                "FilterBlockTableIterator SkipEmptyData key type(%d)size(%d) value: (%d)", 
                ikey.type, ikey.user_key.size(),  cur_value.size());
        }
     }

      //The current KV doesn't match to range_checker, move to next KV.
      second_level_iter_.Next();
      continue;
    }

    if (second_level_iter_.iter() != nullptr &&
        second_level_iter_.status().IsIncomplete()) {
      //incomplete
      return;
    }
    if(!(second_level_iter_.iter() == nullptr ||
         (!second_level_iter_.Valid() &&
         !second_level_iter_.status().IsIncomplete()))){
          Log(InfoLogLevel::ERROR_LEVEL, info_log_,
              "BIG BUG need to check logic A(%d)B(%d)C(%d)", 
              (second_level_iter_.iter() == nullptr), second_level_iter_.Valid(), second_level_iter_.status().IsIncomplete());
    }
    // Move to next block
    if (!first_level_iter_.Valid()) {
      SetSecondLevelIterator(nullptr);
      return;
    }
    first_level_iter_.Next();
    InitDataBlock();
    if (second_level_iter_.iter() != nullptr) {
      second_level_iter_.SeekToFirst();
    }
  }
}

void FilterBlockTableIterator::SkipEmptyDataBlocksBackward() {
  while (true) {
    if (Valid())
    {
      if (range_checker_ == nullptr){
        return ;
      }
      else{
        Slice current_value = value();
        Slice current_key = key();

        ParsedInternalKey ikey;
        if(ParseInternalKey(current_key, &ikey)){
          if(!IsNeedCheckValueType(ikey.type)){
            if(ikey.type != kTypeDeletion){
            Log(InfoLogLevel::WARN_LEVEL, info_log_,
                  "FilterChecker skip Backward key: type(%d)size(%d) value: (%d)", 
                  ikey.type, ikey.user_key.size(), current_value.size());
            }
            return;
          }else if(range_checker_->DoesRecordBelongToChunk(ikey.user_key, current_value)){
          //Range_checker is not appointed or the current KV matches to it.
          return;
          }
        Log(InfoLogLevel::DEBUG_LEVEL, info_log_,
                "FilterBlockTableIterator SkipEmptyData key(%d)size(%d) value: (%d)", 
                ikey.type, ikey.user_key.size(), current_value.size());
        }
      }

      //The current KV doesn't match to range_checker, move to previous KV.
      second_level_iter_.Prev();
      continue;
    }

    if (second_level_iter_.iter() != nullptr &&
        second_level_iter_.status().IsIncomplete()) {
      //incomplete
      return;
    }
    if(!(second_level_iter_.iter() == nullptr ||
         (!second_level_iter_.Valid() &&
         !second_level_iter_.status().IsIncomplete()))){
      Log(InfoLogLevel::ERROR_LEVEL, info_log_,
              "BIG BUG need to check logic A(%d)B(%d)C(%d)", 
              (second_level_iter_.iter() == nullptr), second_level_iter_.Valid(), second_level_iter_.status().IsIncomplete());
    }
    // Move to previous block
    if (!first_level_iter_.Valid()) {
      SetSecondLevelIterator(nullptr);
      return;
    }
    first_level_iter_.Prev();
    InitDataBlock();
    if (second_level_iter_.iter() != nullptr) {
      second_level_iter_.SeekToLast();
    }
  }
}

void FilterBlockTableIterator::SeekForPrev(const Slice& target) {
  if (state_->check_prefix_may_match && !state_->PrefixMayMatch(target)) {
    SetSecondLevelIterator(nullptr);
    return;
  }
  first_level_iter_.Seek(target);
  InitDataBlock();
  if (second_level_iter_.iter() != nullptr) {
    second_level_iter_.SeekForPrev(target);
  }
  SkipNotBelongDataBlocksPre();
  if (!Valid()) {
    if (!first_level_iter_.Valid()) {
      first_level_iter_.SeekToLast();
      InitDataBlock();
      if (second_level_iter_.iter() != nullptr) {
        second_level_iter_.SeekForPrev(target);
      }
    }
    SkipEmptyDataBlocksBackward();
  }
}


InternalIterator* NewFilterBlockTableIterator(TwoLevelIteratorState* state,
                                      InternalIterator* first_level_iter,
                                      Arena* arena,
                                      bool need_free_iter_and_state) {
  if (arena == nullptr) {
    return new FilterBlockTableIterator(state, first_level_iter,
                                need_free_iter_and_state);
  } else {
    auto mem = arena->AllocateAligned(sizeof(FilterBlockTableIterator));
    return new (mem)
        FilterBlockTableIterator(state, first_level_iter, need_free_iter_and_state);
  }
}


InternalIterator* FilterBlockTableReader::NewIterator(const ReadOptions& read_options,
                                               Arena* arena,
                                               bool skip_filters) {
  FilterBlockTableIterator* iterator = reinterpret_cast <FilterBlockTableIterator*>(
    NewFilterBlockTableIterator(
      new BlockEntryIteratorState(this, read_options, skip_filters),
      BlockBasedTable::NewIndexIterator(read_options), arena));
  iterator->SetRangeChecker(GetIOptions().range_checker, GetIOptions().info_log);
  return iterator;
}

Status FilterBlockTableReader::Get(const ReadOptions& readOptions, const Slice& key,
                              GetContext* get_context, bool skip_filters) {
  Status s;

  s = BlockBasedTable::Get(readOptions, key, get_context, skip_filters);

  if(s.ok() && ((get_context->State()) == GetContext::kFound)) {
    // filter out expired data due to splitting DB
    
    Slice value(get_context->GetValue()->c_str(), get_context->GetValue()->size());
    
    const IKeyRangeChecker* range_checker = GetIOptions().range_checker;
    if(range_checker){
      ParsedInternalKey ikey;
      bool tochunk = (ParseInternalKey(key, &ikey) &&
                 range_checker->DoesRecordBelongToChunk(ikey.user_key, value));
      
      Log(InfoLogLevel::DEBUG_LEVEL, GetIOptions().info_log,
              "range_checker: %p, %s; tochunk(%d)",range_checker, range_checker->GetName(), tochunk);
      if(!tochunk){
         return Status::NotFound();
      }
    }

  }
  return s;
}

}  // namespace rocksdb
#endif
