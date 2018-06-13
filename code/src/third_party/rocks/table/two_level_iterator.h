//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include "rocksdb/iterator.h"
#include "rocksdb/env.h"
#include "table/iterator_wrapper.h"
#include "db/pinned_iterators_manager.h"

namespace rocksdb {

struct ReadOptions;
class InternalKeyComparator;
class Arena;

struct TwoLevelIteratorState {
  explicit TwoLevelIteratorState(bool _check_prefix_may_match)
      : check_prefix_may_match(_check_prefix_may_match) {}

  virtual ~TwoLevelIteratorState() {}
  virtual InternalIterator* NewSecondaryIterator(const Slice& handle) = 0;
  virtual bool PrefixMayMatch(const Slice& internal_key) = 0;

  // If call PrefixMayMatch()
  bool check_prefix_may_match;
};
//move from .cc to here by split-feature: support split DB
class TwoLevelIterator : public InternalIterator {
 public:
  explicit TwoLevelIterator(TwoLevelIteratorState* state,
                            InternalIterator* first_level_iter,
                            bool need_free_iter_and_state);

  virtual ~TwoLevelIterator() {
    // Assert that the TwoLevelIterator is never deleted while Pinning is
    // Enabled.
    assert(!pinned_iters_mgr_ ||
           (pinned_iters_mgr_ && !pinned_iters_mgr_->PinningEnabled()));
    first_level_iter_.DeleteIter(!need_free_iter_and_state_);
    second_level_iter_.DeleteIter(false);
    if (need_free_iter_and_state_) {
      delete state_;
    } else {
      state_->~TwoLevelIteratorState();
    }
  }

  virtual void Seek(const Slice& target) override;
  virtual void SeekForPrev(const Slice& target) override;
  virtual void SeekToFirst() override;
  virtual void SeekToLast() override;
  virtual void Next() override;
  virtual void Prev() override;

  virtual bool Valid() const override { return second_level_iter_.Valid(); }
  virtual Slice key() const override {
    assert(Valid());
    return second_level_iter_.key();
  }
  virtual Slice value() const override {
    assert(Valid());
    return second_level_iter_.value();
  }
  virtual Status status() const override {
    // It'd be nice if status() returned a const Status& instead of a Status
    if (!first_level_iter_.status().ok()) {
      return first_level_iter_.status();
    } else if (second_level_iter_.iter() != nullptr &&
               !second_level_iter_.status().ok()) {
      return second_level_iter_.status();
    } else {
      return status_;
    }
  }
  virtual void SetPinnedItersMgr(
      PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
    first_level_iter_.SetPinnedItersMgr(pinned_iters_mgr);
    if (second_level_iter_.iter()) {
      second_level_iter_.SetPinnedItersMgr(pinned_iters_mgr);
    }
  }
  virtual bool IsKeyPinned() const override {
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           second_level_iter_.iter() && second_level_iter_.IsKeyPinned();
  }
  virtual bool IsValuePinned() const override {
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           second_level_iter_.iter() && second_level_iter_.IsValuePinned();
  }

 protected:
  void SaveError(const Status& s) {
    if (status_.ok() && !s.ok()) status_ = s;
  }
  virtual void SkipEmptyDataBlocksForward();
  virtual void SkipEmptyDataBlocksBackward();
  void SetSecondLevelIterator(InternalIterator* iter);
  void InitDataBlock();

  TwoLevelIteratorState* state_;
  IteratorWrapper first_level_iter_;
  IteratorWrapper second_level_iter_;  // May be nullptr
  bool need_free_iter_and_state_;
  PinnedIteratorsManager* pinned_iters_mgr_;
  Status status_;
  // If second_level_iter is non-nullptr, then "data_block_handle_" holds the
  // "index_value" passed to block_function_ to create the second_level_iter.
  std::string data_block_handle_;
};

// Return a new two level iterator.  A two-level iterator contains an
// index iterator whose values point to a sequence of blocks where
// each block is itself a sequence of key,value pairs.  The returned
// two-level iterator yields the concatenation of all key/value pairs
// in the sequence of blocks.  Takes ownership of "index_iter" and
// will delete it when no longer needed.
//
// Uses a supplied function to convert an index_iter value into
// an iterator over the contents of the corresponding block.
// arena: If not null, the arena is used to allocate the Iterator.
//        When destroying the iterator, the destructor will destroy
//        all the states but those allocated in arena.
// need_free_iter_and_state: free `state` and `first_level_iter` if
//                           true. Otherwise, just call destructor.
extern InternalIterator* NewTwoLevelIterator(
    TwoLevelIteratorState* state, InternalIterator* first_level_iter,
    Arena* arena = nullptr, bool need_free_iter_and_state = true);

}  // namespace rocksdb
