//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "rocksdb/status.h"
#include <stdio.h>
#include <cstring>
#include "port/port.h"

#include "common/common.h"
namespace rocksdb {

const char* Status::CopyState(const char* state) {
  char* const result =
      new char[std::strlen(state) + 1];  // +1 for the null terminator
  CommonStrCopy(result, std::strlen(state) + 1, state);
  return result;
}

Status::Status(Code _code, SubCode _subcode, const Slice& msg, const Slice& msg2)
    : code_(_code), subcode_(_subcode) {
  assert(code_ != kOk);
  assert(subcode_ != kMaxSubCode);
  const size_t len1 = msg.size();
  const size_t len2 = msg2.size();
  const size_t size = len1 + (len2 ? (2 + len2) : 0);
  char* const result = new char[size + 1];  // +1 for null terminator
  CommonMemCopy(result, size + 1, msg.data(), len1);
  if (len2) {
    result[len1] = ':';
    result[len1 + 1] = ' ';
    CommonMemCopy(result + len1 + 2, len2, msg2.data(), len2);
  }
  result[size] = '\0';  // null terminator for C style string
  state_ = result;
}

std::string Status::ToString() const {
  char tmp[30];
  const char* type;
  switch (code_) {
    case kOk:
      return "OK";
    case kNotFound:
      type = "Rocksdb NotFound: ";
      break;
    case kCorruption:
      type = "Rocksdb Corruption: ";
      break;
    case kNotSupported:
      type = "Rocksdb Not implemented: ";
      break;
    case kInvalidArgument:
      type = "Rocksdb Invalid argument: ";
      break;
    case kIOError:
      type = "Rocksdb IO error: ";
      break;
    case kMergeInProgress:
      type = "Rocksdb Merge in progress: ";
      break;
    case kIncomplete:
      type = "Rocksdb Result incomplete: ";
      break;
    case kShutdownInProgress:
      type = "Rocksdb Shutdown in progress: ";
      break;
    case kTimedOut:
      type = "Rocksdb Operation timed out: ";
      break;
    case kAborted:
      type = "Rocksdb Operation aborted: ";
      break;
    case kBusy:
      type = "Rocksdb Resource busy: ";
      break;
    case kExpired:
      type = "Rocksdb Operation expired: ";
      break;
    case kTryAgain:
      type = "Rocksdb Operation failed. Try again.: ";
      break;
    default:
      CommonSnprintf(tmp, sizeof(tmp), sizeof(tmp)-1, "Rocksdb Unknown code(%d): ",
               static_cast<int>(code()));
      type = tmp;
      break;
  }
  std::string result(type);
  if (subcode_ != kNone) {
    uint32_t index = static_cast<int32_t>(subcode_);
    assert(sizeof(msgs) > index);
    result.append(msgs[index]);
  }

  if (state_ != nullptr) {
    result.append(state_);
  }
  return result;
}

}  // namespace rocksdb
