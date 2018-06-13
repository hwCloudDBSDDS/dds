//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"
#include "util/logging.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace rocksdb {

class GenerateLevelFilesBriefTest : public testing::Test {
 public:
  std::vector<FileMetaData*> files_;
  LevelFilesBrief file_level_;
  Arena arena_;

  GenerateLevelFilesBriefTest() { }

  ~GenerateLevelFilesBriefTest() {
    for (size_t i = 0; i < files_.size(); i++) {
      delete files_[i];
    }
  }

  void Add(const char* smallest, const char* largest,
           SequenceNumber smallest_seq = 100,
           SequenceNumber largest_seq = 100) {
    FileMetaData* f = new FileMetaData;
    f->fd = FileDescriptor(files_.size() + 1, 0, 0);
    f->smallest = InternalKey(smallest, smallest_seq, kTypeValue);
    f->largest = InternalKey(largest, largest_seq, kTypeValue);
    files_.push_back(f);
  }

  int Compare() {
    int diff = 0;
    for (size_t i = 0; i < files_.size(); i++) {
      if (file_level_.files[i].fd.GetNumber() != files_[i]->fd.GetNumber()) {
        diff++;
      }
    }
    return diff;
  }
};

TEST_F(GenerateLevelFilesBriefTest, Empty) {
  DoGenerateLevelFilesBrief(&file_level_, files_, &arena_);
  ASSERT_EQ(0u, file_level_.num_files);
  ASSERT_EQ(0, Compare());
}

TEST_F(GenerateLevelFilesBriefTest, Single) {
  Add("p", "q");
  DoGenerateLevelFilesBrief(&file_level_, files_, &arena_);
  ASSERT_EQ(1u, file_level_.num_files);
  ASSERT_EQ(0, Compare());
}

TEST_F(GenerateLevelFilesBriefTest, Multiple) {
  Add("150", "200");
  Add("200", "250");
  Add("300", "350");
  Add("400", "450");
  DoGenerateLevelFilesBrief(&file_level_, files_, &arena_);
  ASSERT_EQ(4u, file_level_.num_files);
  ASSERT_EQ(0, Compare());
}

class CountingLogger : public Logger {
 public:
  CountingLogger() : log_count(0) {}
  using Logger::Logv;
  virtual void Logv(const char* format, va_list ap) override { log_count++; }
  int log_count;
};

Options GetOptionsWithNumLevels(int num_levels,
                                std::shared_ptr<CountingLogger> logger) {
  Options opt;
  opt.num_levels = num_levels;
  opt.info_log = logger;
  return opt;
}

class VersionStorageInfoTest : public testing::Test {
 public:
  const Comparator* ucmp_;
  InternalKeyComparator icmp_;
  std::shared_ptr<CountingLogger> logger_;
  Options options_;
  ImmutableCFOptions ioptions_;
  MutableCFOptions mutable_cf_options_;
  VersionStorageInfo vstorage_;

  InternalKey GetInternalKey(const char* ukey,
                             SequenceNumber smallest_seq = 100) {
    return InternalKey(ukey, smallest_seq, kTypeValue);
  }

  VersionStorageInfoTest()
      : ucmp_(BytewiseComparator()),
        icmp_(ucmp_),
        logger_(new CountingLogger()),
        options_(GetOptionsWithNumLevels(6, logger_)),
        ioptions_(options_),
        mutable_cf_options_(options_),
        vstorage_(&icmp_, ucmp_, 6, kCompactionStyleLevel, nullptr, false) {}

  ~VersionStorageInfoTest() {
    for (int i = 0; i < vstorage_.num_levels(); i++) {
      for (auto* f : vstorage_.LevelFiles(i)) {
        if (--f->refs == 0) {
          delete f;
        }
      }
    }
  }

  void Add(int level, uint32_t file_number, const char* smallest,
           const char* largest, uint64_t file_size = 0) {
    assert(level < vstorage_.num_levels());
    FileMetaData* f = new FileMetaData;
    f->fd = FileDescriptor(file_number, 0, file_size);
    f->smallest = GetInternalKey(smallest, 0);
    f->largest = GetInternalKey(largest, 0);
    f->compensated_file_size = file_size;
    f->refs = 0;
    f->num_entries = num_entries_;
    f->num_deletions = num_deletions_;
    vstorage_.AddFile(level, f);
  }

// Memebers and methods for preparing levels file by random.
  std::vector<uint32_t> keys_;
  uint32_t keys_num_;
  uint64_t file_size_;
  uint32_t key_digits_num_;
  const uint64_t num_entries_ = 2;
  const uint64_t num_deletions_ = 1;

  // Get the key string by index.
  std::string GetKeyStr(const int key_index) {
    char key_fmt[32] = {0};
    char key_str[32] = {0};
    sprintf(key_fmt, "%%0%ud", key_digits_num_); //like "%08d"
    sprintf(key_str, key_fmt, keys_[key_index]);
    return std::string(key_str);
  }
  std::string GetSmallestKeyStr(const int key_index) {
    return GetKeyStr(key_index);
  }
  std::string GetLargestKeyStr(const int key_index) {
    char fmt[32] = {0};
    char key_str[32] = {0};
    sprintf(fmt, "%%0%ud", key_digits_num_);
    sprintf(key_str, fmt, (keys_[key_index+1] - 1));
    return std::string(key_str);
  }

  // Get the split point by size.
  void GetSplitPointBySize(std::string& split_point,
                           uint64_t& left_datasize,
                           uint64_t& left_numrecord,
                           uint64_t& right_datasize,
                           uint64_t& right_numrecord) {
    int split_point_index  = ((keys_num_- 1) / 2);
    int left_files_num = split_point_index + 1;
    split_point = GetLargestKeyStr(split_point_index);
    left_datasize = left_files_num * file_size_;
    left_numrecord = left_files_num * (num_entries_ - num_deletions_);
    right_datasize = (keys_num_ - left_files_num) * file_size_;
    right_numrecord = (keys_num_ - left_files_num) * (num_entries_ - num_deletions_);
  }

  // Select the split point by random.
  void SelectSplitPointByRandom(std::string& split_point,
                                uint64_t& left_datasize,
                                uint64_t& left_numrecord,
                                uint64_t& right_datasize,
                                uint64_t& right_numrecord) {
    int split_point_index  = rand() % keys_num_;
    int left_files_num = split_point_index + 1;
    split_point = GetLargestKeyStr(split_point_index);
    left_datasize = left_files_num * file_size_;
    left_numrecord = left_files_num * (num_entries_ - num_deletions_);
    right_datasize = (keys_num_ - left_files_num) * file_size_;
    right_numrecord = (keys_num_ - left_files_num) * (num_entries_ - num_deletions_);
  }

  // Prepare levels file by random.
  // Can only be called once at initial step in one test case because there is no clearing function.
  void PrepareLevelsFileByRandom(const uint64_t keys_num, const uint64_t file_size) {
    uint32_t file_sn = 1;
    int level = 0;

    // Generate keys in order.
    srand((unsigned)time(NULL));
    keys_.reserve(keys_num+1);
    for (uint32_t i = 0; i < (keys_num+1); i++) {
      keys_[i] = i*10 + 1;
    }
    keys_num_ = keys_num;
    file_size_ = file_size;

    // Get the maximum number of digits in a key.
    uint64_t num = keys_num;
    key_digits_num_ = 0;
    while (num > 0) {
      key_digits_num_++;
      num = num / 10;
    }
    key_digits_num_ += 2;

    // Generate levels file by random.
    for (uint32_t i = 0; i < keys_num; i++) {
      level = ((rand() % 3) + 1);
      Add(level, file_sn, GetSmallestKeyStr(i).c_str(), GetLargestKeyStr(i).c_str(), file_size);
      file_sn++;
    }
  }

};

TEST_F(VersionStorageInfoTest, MaxBytesForLevelStatic) {
  ioptions_.level_compaction_dynamic_level_bytes = false;
  mutable_cf_options_.max_bytes_for_level_base = 10;
  mutable_cf_options_.max_bytes_for_level_multiplier = 5;
  Add(4, 100U, "1", "2");
  Add(5, 101U, "1", "2");

  vstorage_.CalculateBaseBytes(ioptions_, mutable_cf_options_);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(1), 10U);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(2), 50U);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(3), 250U);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(4), 1250U);

  ASSERT_EQ(0, logger_->log_count);
}

TEST_F(VersionStorageInfoTest, MaxBytesForLevelDynamic) {
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.max_bytes_for_level_base = 1000;
  mutable_cf_options_.max_bytes_for_level_multiplier = 5;
  Add(5, 1U, "1", "2", 500U);

  vstorage_.CalculateBaseBytes(ioptions_, mutable_cf_options_);
  ASSERT_EQ(0, logger_->log_count);
  ASSERT_EQ(vstorage_.base_level(), 5);

  Add(5, 2U, "3", "4", 550U);
  vstorage_.CalculateBaseBytes(ioptions_, mutable_cf_options_);
  ASSERT_EQ(0, logger_->log_count);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(4), 210U);
  ASSERT_EQ(vstorage_.base_level(), 4);

  Add(4, 3U, "3", "4", 550U);
  vstorage_.CalculateBaseBytes(ioptions_, mutable_cf_options_);
  ASSERT_EQ(0, logger_->log_count);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(4), 210U);
  ASSERT_EQ(vstorage_.base_level(), 4);

  Add(3, 4U, "3", "4", 250U);
  Add(3, 5U, "5", "7", 300U);
  vstorage_.CalculateBaseBytes(ioptions_, mutable_cf_options_);
  ASSERT_EQ(1, logger_->log_count);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(4), 1005U);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(3), 201U);
  ASSERT_EQ(vstorage_.base_level(), 3);

  Add(1, 6U, "3", "4", 5U);
  Add(1, 7U, "8", "9", 5U);
  logger_->log_count = 0;
  vstorage_.CalculateBaseBytes(ioptions_, mutable_cf_options_);
  ASSERT_EQ(1, logger_->log_count);
  ASSERT_GT(vstorage_.MaxBytesForLevel(4), 1005U);
  ASSERT_GT(vstorage_.MaxBytesForLevel(3), 1005U);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(2), 1005U);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(1), 201U);
  ASSERT_EQ(vstorage_.base_level(), 1);
}

TEST_F(VersionStorageInfoTest, MaxBytesForLevelDynamicLotsOfData) {
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.max_bytes_for_level_base = 100;
  mutable_cf_options_.max_bytes_for_level_multiplier = 2;
  Add(0, 1U, "1", "2", 50U);
  Add(1, 2U, "1", "2", 50U);
  Add(2, 3U, "1", "2", 500U);
  Add(3, 4U, "1", "2", 500U);
  Add(4, 5U, "1", "2", 1700U);
  Add(5, 6U, "1", "2", 500U);

  vstorage_.CalculateBaseBytes(ioptions_, mutable_cf_options_);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(4), 800U);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(3), 400U);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(2), 200U);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(1), 100U);
  ASSERT_EQ(vstorage_.base_level(), 1);
  ASSERT_EQ(0, logger_->log_count);
}

TEST_F(VersionStorageInfoTest, MaxBytesForLevelDynamicLargeLevel) {
  uint64_t kOneGB = 1000U * 1000U * 1000U;
  ioptions_.level_compaction_dynamic_level_bytes = true;
  mutable_cf_options_.max_bytes_for_level_base = 10U * kOneGB;
  mutable_cf_options_.max_bytes_for_level_multiplier = 10;
  Add(0, 1U, "1", "2", 50U);
  Add(3, 4U, "1", "2", 32U * kOneGB);
  Add(4, 5U, "1", "2", 500U * kOneGB);
  Add(5, 6U, "1", "2", 3000U * kOneGB);

  vstorage_.CalculateBaseBytes(ioptions_, mutable_cf_options_);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(5), 3000U * kOneGB);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(4), 300U * kOneGB);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(3), 30U * kOneGB);
  ASSERT_EQ(vstorage_.MaxBytesForLevel(2), 3U * kOneGB);
  ASSERT_EQ(vstorage_.base_level(), 2);
  ASSERT_EQ(0, logger_->log_count);
}

TEST_F(VersionStorageInfoTest, EstimateLiveDataSize) {
  // Test whether the overlaps are detected as expected
  Add(1, 1U, "4", "7", 1U);  // Perfect overlap with last level
  Add(2, 2U, "3", "5", 1U);  // Partial overlap with last level
  Add(2, 3U, "6", "8", 1U);  // Partial overlap with last level
  Add(3, 4U, "1", "9", 1U);  // Contains range of last level
  Add(4, 5U, "4", "5", 1U);  // Inside range of last level
  Add(4, 5U, "6", "7", 1U);  // Inside range of last level
  Add(5, 6U, "4", "7", 10U);
  ASSERT_EQ(10U, vstorage_.EstimateLiveDataSize());
}

TEST_F(VersionStorageInfoTest, EstimateLiveDataSize2) {
  Add(0, 1U, "9", "9", 1U);  // Level 0 is not ordered
  Add(0, 1U, "5", "6", 1U);  // Ignored because of [5,6] in l1
  Add(1, 1U, "1", "2", 1U);  // Ignored because of [2,3] in l2
  Add(1, 2U, "3", "4", 1U);  // Ignored because of [2,3] in l2
  Add(1, 3U, "5", "6", 1U);
  Add(2, 4U, "2", "3", 1U);
  Add(3, 5U, "7", "8", 1U);
  ASSERT_EQ(4U, vstorage_.EstimateLiveDataSize());
}

// Test cases for invalid split:
// 1) No file
TEST_F(VersionStorageInfoTest, GetSplitPointBySize_NoFile) {
  // GetSplitPointBySize() will ignore all the files at level 0.
  uint64_t file_size = 1023;
  uint32_t file_sn = 1;
  int level = 0;

  // GetSplitPointBySize() will ignore all the files at level 0.
  Add(level, file_sn, "0000", "0101", file_size);  file_sn++;
  Add(level, file_sn, "0102", "0901", file_size);  file_sn++;
  Add(level, file_sn, "0902", "1301", file_size);  file_sn++;

  std::string split_point;
  uint64_t left_datasize = 321;
  uint64_t left_numrecord = 123;
  uint64_t right_datasize = 456;;
  uint64_t right_numrecord = 654;
  Status s = vstorage_.GetSplitPointBySize(nullptr,
                                           split_point,
                                           left_datasize,
                                           left_numrecord,
                                           right_datasize,
                                           right_numrecord);
  ASSERT_EQ(true, s.IsNotFound());
  ASSERT_EQ("", split_point);
  ASSERT_EQ(0, left_datasize);
  ASSERT_EQ(0, left_numrecord);
  ASSERT_EQ(0, right_datasize);
  ASSERT_EQ(0, right_numrecord);
}
// 2) Only one file
TEST_F(VersionStorageInfoTest, GetSplitPointBySize_OnlyOneFile) {
  uint64_t file_size = 1023;
  uint32_t file_sn = 1;
  int level = 0;

  // GetSplitPointBySize() will ignore all the files at level 0.
  Add(level, file_sn, "0000", "0101", file_size);  file_sn++;
  Add(level, file_sn, "0102", "0901", file_size);  file_sn++;
  Add(level, file_sn, "0902", "1301", file_size);  file_sn++;

  level = 2;
  Add(level, file_sn, "0100", "0701", file_size);  file_sn++;

  std::string split_point;
  uint64_t left_datasize = 321;
  uint64_t left_numrecord = 123;
  uint64_t right_datasize = 456;;
  uint64_t right_numrecord = 654;
  Status s = vstorage_.GetSplitPointBySize(nullptr,
                                           split_point,
                                           left_datasize,
                                           left_numrecord,
                                           right_datasize,
                                           right_numrecord);
  ASSERT_EQ(true, s.IsNotFound());
  ASSERT_EQ("", split_point);
  ASSERT_EQ(0, left_datasize);
  ASSERT_EQ(0, left_numrecord);
  ASSERT_EQ(0, right_datasize);
  ASSERT_EQ(0, right_numrecord);
}
// 3) The total size of all the other files except the largest one is smaller than that of the largest one
TEST_F(VersionStorageInfoTest, GetSplitPointBySize_NotFound) {
  uint64_t file_size = 1023;
  uint32_t file_sn = 1;
  int level = 0;

  // GetSplitPointBySize() will ignore all the files at level 0.
  Add(level, file_sn, "0000", "0101", file_size);  file_sn++;
  Add(level, file_sn, "0102", "0901", file_size);  file_sn++;
  Add(level, file_sn, "0902", "1301", file_size);  file_sn++;

  level = 2;
  Add(level, file_sn, "0100", "0201", 205);  file_sn++;
  Add(level, file_sn, "0202", "0701", 205);  file_sn++;

  level = 3;
  Add(level, file_sn, "0000", "0301", 205);  file_sn++;
  Add(level, file_sn, "0302", "2501", 1027);  file_sn++; // the largest file

  level = 5;
  Add(level, file_sn, "0302", "1001", 205);  file_sn++;
  Add(level, file_sn, "1002", "1601", 205);  file_sn++;

  std::string split_point;
  uint64_t left_datasize = 321;
  uint64_t left_numrecord = 123;
  uint64_t right_datasize = 456;;
  uint64_t right_numrecord = 654;
  Status s = vstorage_.GetSplitPointBySize(nullptr,
                                           split_point,
                                           left_datasize,
                                           left_numrecord,
                                           right_datasize,
                                           right_numrecord);
  ASSERT_EQ(true, s.IsNotFound());
  ASSERT_EQ("", split_point);
  ASSERT_EQ(0, left_datasize);
  ASSERT_EQ(0, left_numrecord);
  ASSERT_EQ(0, right_datasize);
  ASSERT_EQ(0, right_numrecord);
}

// Test cases for valid split:
// 1) Normal test case with some boundary condition such as duplicate largest key in diffrent files, empty level existed.
TEST_F(VersionStorageInfoTest, GetSplitPointBySize_Normal) {
  uint64_t file_size = 1023;
  uint32_t file_sn = 1;
  int level = 0;

  // GetSplitPointBySize() will ignore all the files at level 0.
  Add(level, file_sn, "0000", "0101", file_size);  file_sn++;
  Add(level, file_sn, "0102", "0901", file_size);  file_sn++;
  Add(level, file_sn, "0902", "1301", file_size);  file_sn++;

  level = 2;
  Add(level, file_sn, "0100", "0701", file_size);  file_sn++; //0701 duplicate largest key in different files
  Add(level, file_sn, "0702", "1401", file_size);  file_sn++;
  Add(level, file_sn, "1402", "1501", file_size);  file_sn++; //1501 duplicate largest key in different files
  Add(level, file_sn, "1502", "1601", file_size);  file_sn++;
  Add(level, file_sn, "1602", "1701", file_size);  file_sn++;

  level = 3;
  Add(level, file_sn, "0000", "0301", file_size);  file_sn++;
  Add(level, file_sn, "0302", "0401", file_size);  file_sn++; //0401 duplicate largest key in different files
  Add(level, file_sn, "0402", "0701", file_size);  file_sn++; //0701 duplicate largest key in different files
  Add(level, file_sn, "0702", "0801", file_size);  file_sn++;
  Add(level, file_sn, "0802", "0901", file_size);  file_sn++;
  Add(level, file_sn, "0902", "1201", file_size);  file_sn++; //1201 duplicate largest key in different files
  Add(level, file_sn, "1202", "1301", file_size);  file_sn++;
  Add(level, file_sn, "1302", "2001", file_size);  file_sn++;
  Add(level, file_sn, "2002", "2101", file_size);  file_sn++;

  level = 5;
  Add(level, file_sn, "0000", "0101", file_size);  file_sn++;
  Add(level, file_sn, "0102", "0201", file_size);  file_sn++;
  Add(level, file_sn, "0202", "0401", file_size);  file_sn++; //0401 duplicate largest key in different files
  Add(level, file_sn, "0402", "0501", file_size);  file_sn++;
  Add(level, file_sn, "0502", "0601", file_size);  file_sn++;
  Add(level, file_sn, "0602", "1001", file_size);  file_sn++;
  Add(level, file_sn, "1002", "1101", file_size);  file_sn++;
  Add(level, file_sn, "1102", "1201", file_size);  file_sn++; //1201 duplicate largest key in different files
  Add(level, file_sn, "1202", "1501", file_size);  file_sn++; //1501 duplicate largest key in different files
  Add(level, file_sn, "1502", "1801", file_size);  file_sn++;
  Add(level, file_sn, "1802", "1901", file_size);  file_sn++;
  Add(level, file_sn, "1902", "2201", file_size);  file_sn++;
  Add(level, file_sn, "2202", "2301", file_size);  file_sn++;

  std::string split_point;
  uint64_t left_datasize = 321;
  uint64_t left_numrecord = 123;
  uint64_t right_datasize = 456;;
  uint64_t right_numrecord = 654;
  Status s = vstorage_.GetSplitPointBySize(nullptr,
                                           split_point,
                                           left_datasize,
                                           left_numrecord,
                                           right_datasize,
                                           right_numrecord);
  ASSERT_EQ(true, s.ok());
  ASSERT_EQ("1201", split_point);
  ASSERT_EQ(14322, left_datasize);  // 14*1023
  ASSERT_EQ(14, left_numrecord);    // 14*(2-1)
  ASSERT_EQ(13299, right_datasize); // 13*1023
  ASSERT_EQ(13, right_numrecord);   // 13*(2-1)
}
// 2) Test case for random data
TEST_F(VersionStorageInfoTest, GetSplitPointBySize_RandomData) {
  // Prepare levels file by random.
  PrepareLevelsFileByRandom(100000, 1023);

  // Get the expected result.
  std::string split_point_expected;
  uint64_t left_datasize_expected = 321;
  uint64_t left_numrecord_expected = 123;
  uint64_t right_datasize_expected = 456;;
  uint64_t right_numrecord_expected = 654;
  GetSplitPointBySize(split_point_expected,
                      left_datasize_expected,
                      left_numrecord_expected,
                      right_datasize_expected,
                      right_numrecord_expected);

  //Check result
  std::string split_point;
  uint64_t left_datasize = 321;
  uint64_t left_numrecord = 123;
  uint64_t right_datasize = 456;;
  uint64_t right_numrecord = 654;
  Status s = vstorage_.GetSplitPointBySize(nullptr,
                                           split_point,
                                           left_datasize,
                                           left_numrecord,
                                           right_datasize,
                                           right_numrecord);
  ASSERT_EQ(true, s.ok());
  ASSERT_EQ(split_point_expected, split_point);
  ASSERT_EQ(left_datasize_expected, left_datasize);
  ASSERT_EQ(left_numrecord_expected, left_numrecord);
  ASSERT_EQ(right_datasize_expected, right_datasize);
  ASSERT_EQ(right_numrecord_expected, right_numrecord);
}

// Test cases for get split information by split point:
// 1) Normal test case with some boundary condition such as duplicate largest key in diffrent files, empty level existed.
TEST_F(VersionStorageInfoTest, GetSplitInfoBySplitPoint) {
  uint64_t file_size = 1023;
  uint32_t file_sn = 1;
  int level = 0;

  // GetSplitPointBySize() will ignore all the files at level 0.
  Add(level, file_sn, "0000", "0101", file_size);  file_sn++;
  Add(level, file_sn, "0102", "0901", file_size);  file_sn++;
  Add(level, file_sn, "0902", "1301", file_size);  file_sn++;

  level = 2;
  Add(level, file_sn, "0100", "0701", file_size);  file_sn++; //0701 duplicate largest key in different files
  Add(level, file_sn, "0702", "1401", file_size);  file_sn++;
  Add(level, file_sn, "1402", "1501", file_size);  file_sn++; //1501 duplicate largest key in different files
  Add(level, file_sn, "1502", "1601", file_size);  file_sn++;
  Add(level, file_sn, "1602", "1701", file_size);  file_sn++;

  level = 3;
  Add(level, file_sn, "0000", "0301", file_size);  file_sn++;
  Add(level, file_sn, "0302", "0401", file_size);  file_sn++; //0401 duplicate largest key in different files
  Add(level, file_sn, "0402", "0701", file_size);  file_sn++; //0701 duplicate largest key in different files
  Add(level, file_sn, "0702", "0801", file_size);  file_sn++;
  Add(level, file_sn, "0802", "0901", file_size);  file_sn++;
  Add(level, file_sn, "0902", "1201", file_size);  file_sn++; //1201 duplicate largest key in different files
  Add(level, file_sn, "1202", "1301", file_size);  file_sn++;
  Add(level, file_sn, "1302", "2001", file_size);  file_sn++;
  Add(level, file_sn, "2002", "2101", file_size);  file_sn++;

  level = 5;
  Add(level, file_sn, "0000", "0101", file_size);  file_sn++;
  Add(level, file_sn, "0102", "0201", file_size);  file_sn++;
  Add(level, file_sn, "0202", "0401", file_size);  file_sn++; //0401 duplicate largest key in different files
  Add(level, file_sn, "0402", "0501", file_size);  file_sn++;
  Add(level, file_sn, "0502", "0601", file_size);  file_sn++;
  Add(level, file_sn, "0602", "1001", file_size);  file_sn++;
  Add(level, file_sn, "1002", "1101", file_size);  file_sn++;
  Add(level, file_sn, "1102", "1201", file_size);  file_sn++; //1201 duplicate largest key in different files
  Add(level, file_sn, "1202", "1501", file_size);  file_sn++; //1501 duplicate largest key in different files
  Add(level, file_sn, "1502", "1801", file_size);  file_sn++;
  Add(level, file_sn, "1802", "1901", file_size);  file_sn++;
  Add(level, file_sn, "1902", "2201", file_size);  file_sn++;
  Add(level, file_sn, "2202", "2301", file_size);  file_sn++;

  std::string split_point = "";
  uint64_t left_datasize = 321;
  uint64_t left_numrecord = 123;
  uint64_t right_datasize = 456;;
  uint64_t right_numrecord = 654;
  Status s;

  split_point = "1200";
  s = vstorage_.GetSplitInfoBySplitPoint(nullptr,
                                         split_point,
                                         left_datasize,
                                         left_numrecord,
                                         right_datasize,
                                         right_numrecord);
  ASSERT_EQ(true, s.ok());
  ASSERT_EQ(13299, left_datasize);  // 13*1023
  ASSERT_EQ(13, left_numrecord);    // 13*(2-1)
  ASSERT_EQ(14322, right_datasize); // 14*1023
  ASSERT_EQ(14, right_numrecord);   // 14*(2-1)

  split_point = "1201";
  s = vstorage_.GetSplitInfoBySplitPoint(nullptr,
                                         split_point,
                                         left_datasize,
                                         left_numrecord,
                                         right_datasize,
                                         right_numrecord);
  ASSERT_EQ(true, s.ok());
  ASSERT_EQ(15345, left_datasize);  // 15*1023
  ASSERT_EQ(15, left_numrecord);    // 15*(2-1)
  ASSERT_EQ(12276, right_datasize); // 12*1023
  ASSERT_EQ(12, right_numrecord);   // 12*(2-1)

  split_point = "1202";
  s = vstorage_.GetSplitInfoBySplitPoint(nullptr,
                                         split_point,
                                         left_datasize,
                                         left_numrecord,
                                         right_datasize,
                                         right_numrecord);
  ASSERT_EQ(true, s.ok());
  ASSERT_EQ(15345, left_datasize);  // 15*1023
  ASSERT_EQ(15, left_numrecord);    // 15*(2-1)
  ASSERT_EQ(12276, right_datasize); // 12*1023
  ASSERT_EQ(12, right_numrecord);   // 12*(2-1)
}
// 2) Test case for random data
TEST_F(VersionStorageInfoTest, GetSplitInfoBySplitPoint_RandomData) {
  // Prepare levels file by random.
  PrepareLevelsFileByRandom(100000, 1023);

  // Select split point by random.
  std::string split_point = "";
  uint64_t left_datasize_expected = 321;
  uint64_t left_numrecord_expected = 123;
  uint64_t right_datasize_expected = 456;;
  uint64_t right_numrecord_expected = 654;
  SelectSplitPointByRandom(split_point,
                           left_datasize_expected,
                           left_numrecord_expected,
                           right_datasize_expected,
                           right_numrecord_expected);

  //Check result
  uint64_t left_datasize = 321;
  uint64_t left_numrecord = 123;
  uint64_t right_datasize = 456;;
  uint64_t right_numrecord = 654;
  Status s;
  s = vstorage_.GetSplitInfoBySplitPoint(nullptr,
                                         split_point,
                                         left_datasize,
                                         left_numrecord,
                                         right_datasize,
                                         right_numrecord);
  ASSERT_EQ(true, s.ok());
  ASSERT_EQ(left_datasize_expected, left_datasize);
  ASSERT_EQ(left_numrecord_expected, left_numrecord);
  ASSERT_EQ(right_datasize_expected, right_datasize);
  ASSERT_EQ(right_numrecord_expected, right_numrecord);
}

class FindLevelFileTest : public testing::Test {
 public:
  LevelFilesBrief file_level_;
  bool disjoint_sorted_files_;
  Arena arena_;

  FindLevelFileTest() : disjoint_sorted_files_(true) { }

  ~FindLevelFileTest() {
  }

  void LevelFileInit(size_t num = 0) {
    char* mem = arena_.AllocateAligned(num * sizeof(FdWithKeyRange));
    file_level_.files = new (mem)FdWithKeyRange[num];
    file_level_.num_files = 0;
  }

  void Add(const char* smallest, const char* largest,
           SequenceNumber smallest_seq = 100,
           SequenceNumber largest_seq = 100) {
    InternalKey smallest_key = InternalKey(smallest, smallest_seq, kTypeValue);
    InternalKey largest_key = InternalKey(largest, largest_seq, kTypeValue);

    Slice smallest_slice = smallest_key.Encode();
    Slice largest_slice = largest_key.Encode();

    char* mem = arena_.AllocateAligned(
        smallest_slice.size() + largest_slice.size());
    memcpy(mem, smallest_slice.data(), smallest_slice.size());
    memcpy(mem + smallest_slice.size(), largest_slice.data(),
        largest_slice.size());

    // add to file_level_
    size_t num = file_level_.num_files;
    auto& file = file_level_.files[num];
    file.fd = FileDescriptor(num + 1, 0, 0);
    file.smallest_key = Slice(mem, smallest_slice.size());
    file.largest_key = Slice(mem + smallest_slice.size(),
        largest_slice.size());
    file_level_.num_files++;
  }

  int Find(const char* key) {
    InternalKey target(key, 100, kTypeValue);
    InternalKeyComparator cmp(BytewiseComparator());
    return FindFile(cmp, file_level_, target.Encode());
  }

  bool Overlaps(const char* smallest, const char* largest) {
    InternalKeyComparator cmp(BytewiseComparator());
    Slice s(smallest != nullptr ? smallest : "");
    Slice l(largest != nullptr ? largest : "");
    return SomeFileOverlapsRange(cmp, disjoint_sorted_files_, file_level_,
                                 (smallest != nullptr ? &s : nullptr),
                                 (largest != nullptr ? &l : nullptr));
  }
};

TEST_F(FindLevelFileTest, LevelEmpty) {
  LevelFileInit(0);

  ASSERT_EQ(0, Find("foo"));
  ASSERT_TRUE(! Overlaps("a", "z"));
  ASSERT_TRUE(! Overlaps(nullptr, "z"));
  ASSERT_TRUE(! Overlaps("a", nullptr));
  ASSERT_TRUE(! Overlaps(nullptr, nullptr));
}

TEST_F(FindLevelFileTest, LevelSingle) {
  LevelFileInit(1);

  Add("p", "q");
  ASSERT_EQ(0, Find("a"));
  ASSERT_EQ(0, Find("p"));
  ASSERT_EQ(0, Find("p1"));
  ASSERT_EQ(0, Find("q"));
  ASSERT_EQ(1, Find("q1"));
  ASSERT_EQ(1, Find("z"));

  ASSERT_TRUE(! Overlaps("a", "b"));
  ASSERT_TRUE(! Overlaps("z1", "z2"));
  ASSERT_TRUE(Overlaps("a", "p"));
  ASSERT_TRUE(Overlaps("a", "q"));
  ASSERT_TRUE(Overlaps("a", "z"));
  ASSERT_TRUE(Overlaps("p", "p1"));
  ASSERT_TRUE(Overlaps("p", "q"));
  ASSERT_TRUE(Overlaps("p", "z"));
  ASSERT_TRUE(Overlaps("p1", "p2"));
  ASSERT_TRUE(Overlaps("p1", "z"));
  ASSERT_TRUE(Overlaps("q", "q"));
  ASSERT_TRUE(Overlaps("q", "q1"));

  ASSERT_TRUE(! Overlaps(nullptr, "j"));
  ASSERT_TRUE(! Overlaps("r", nullptr));
  ASSERT_TRUE(Overlaps(nullptr, "p"));
  ASSERT_TRUE(Overlaps(nullptr, "p1"));
  ASSERT_TRUE(Overlaps("q", nullptr));
  ASSERT_TRUE(Overlaps(nullptr, nullptr));
}

TEST_F(FindLevelFileTest, LevelMultiple) {
  LevelFileInit(4);

  Add("150", "200");
  Add("200", "250");
  Add("300", "350");
  Add("400", "450");
  ASSERT_EQ(0, Find("100"));
  ASSERT_EQ(0, Find("150"));
  ASSERT_EQ(0, Find("151"));
  ASSERT_EQ(0, Find("199"));
  ASSERT_EQ(0, Find("200"));
  ASSERT_EQ(1, Find("201"));
  ASSERT_EQ(1, Find("249"));
  ASSERT_EQ(1, Find("250"));
  ASSERT_EQ(2, Find("251"));
  ASSERT_EQ(2, Find("299"));
  ASSERT_EQ(2, Find("300"));
  ASSERT_EQ(2, Find("349"));
  ASSERT_EQ(2, Find("350"));
  ASSERT_EQ(3, Find("351"));
  ASSERT_EQ(3, Find("400"));
  ASSERT_EQ(3, Find("450"));
  ASSERT_EQ(4, Find("451"));

  ASSERT_TRUE(! Overlaps("100", "149"));
  ASSERT_TRUE(! Overlaps("251", "299"));
  ASSERT_TRUE(! Overlaps("451", "500"));
  ASSERT_TRUE(! Overlaps("351", "399"));

  ASSERT_TRUE(Overlaps("100", "150"));
  ASSERT_TRUE(Overlaps("100", "200"));
  ASSERT_TRUE(Overlaps("100", "300"));
  ASSERT_TRUE(Overlaps("100", "400"));
  ASSERT_TRUE(Overlaps("100", "500"));
  ASSERT_TRUE(Overlaps("375", "400"));
  ASSERT_TRUE(Overlaps("450", "450"));
  ASSERT_TRUE(Overlaps("450", "500"));
}

TEST_F(FindLevelFileTest, LevelMultipleNullBoundaries) {
  LevelFileInit(4);

  Add("150", "200");
  Add("200", "250");
  Add("300", "350");
  Add("400", "450");
  ASSERT_TRUE(! Overlaps(nullptr, "149"));
  ASSERT_TRUE(! Overlaps("451", nullptr));
  ASSERT_TRUE(Overlaps(nullptr, nullptr));
  ASSERT_TRUE(Overlaps(nullptr, "150"));
  ASSERT_TRUE(Overlaps(nullptr, "199"));
  ASSERT_TRUE(Overlaps(nullptr, "200"));
  ASSERT_TRUE(Overlaps(nullptr, "201"));
  ASSERT_TRUE(Overlaps(nullptr, "400"));
  ASSERT_TRUE(Overlaps(nullptr, "800"));
  ASSERT_TRUE(Overlaps("100", nullptr));
  ASSERT_TRUE(Overlaps("200", nullptr));
  ASSERT_TRUE(Overlaps("449", nullptr));
  ASSERT_TRUE(Overlaps("450", nullptr));
}

TEST_F(FindLevelFileTest, LevelOverlapSequenceChecks) {
  LevelFileInit(1);

  Add("200", "200", 5000, 3000);
  ASSERT_TRUE(! Overlaps("199", "199"));
  ASSERT_TRUE(! Overlaps("201", "300"));
  ASSERT_TRUE(Overlaps("200", "200"));
  ASSERT_TRUE(Overlaps("190", "200"));
  ASSERT_TRUE(Overlaps("200", "210"));
}

TEST_F(FindLevelFileTest, LevelOverlappingFiles) {
  LevelFileInit(2);

  Add("150", "600");
  Add("400", "500");
  disjoint_sorted_files_ = false;
  ASSERT_TRUE(! Overlaps("100", "149"));
  ASSERT_TRUE(! Overlaps("601", "700"));
  ASSERT_TRUE(Overlaps("100", "150"));
  ASSERT_TRUE(Overlaps("100", "200"));
  ASSERT_TRUE(Overlaps("100", "300"));
  ASSERT_TRUE(Overlaps("100", "400"));
  ASSERT_TRUE(Overlaps("100", "500"));
  ASSERT_TRUE(Overlaps("375", "400"));
  ASSERT_TRUE(Overlaps("450", "450"));
  ASSERT_TRUE(Overlaps("450", "500"));
  ASSERT_TRUE(Overlaps("450", "700"));
  ASSERT_TRUE(Overlaps("600", "700"));
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
