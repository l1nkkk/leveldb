// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"

namespace leveldb {

// See doc/table_format.md for an explanation of the filter block format.

// kFilterBase = 2KB
// Generate new filter every 2KB of data
static const size_t kFilterBaseLg = 11;
static const size_t kFilterBase = 1 << kFilterBaseLg;

FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {}

void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
  // 传入 data block 的offset，循环判断当前是否需要生成新的 filter Slice（调用GenerateFilter）
  uint64_t filter_index = (block_offset / kFilterBase);
  assert(filter_index >= filter_offsets_.size());
  while (filter_index > filter_offsets_.size()) {
    GenerateFilter();
  }
}

void FilterBlockBuilder::AddKey(const Slice& key) {
  Slice k = key;
  // 注意：这里是keys_，而不是key
  start_.push_back(keys_.size());
  keys_.append(k.data(), k.size());
}

Slice FilterBlockBuilder::Finish() {
  // 1. 如果当前还有缓存的key，则调用 GenerateFilter() 生成新的Slice
  if (!start_.empty()) {
    GenerateFilter();
  }

  /// 2. 将所有的 filter index 添加到 filter data 中 
  // Append array of per-filter offsets
  const uint32_t array_offset = result_.size();
  for (size_t i = 0; i < filter_offsets_.size(); i++) {
    PutFixed32(&result_, filter_offsets_[i]);
  }

  /// 3. 将 filter index 在 filter data 的偏移添加到result_（filter data）中
  PutFixed32(&result_, array_offset);

  /// 4. 添加filter的block offset 阈值相关系数到result_（filter data）中
  result_.push_back(kFilterBaseLg);  // Save encoding parameter in result
  return Slice(result_);
}

void FilterBlockBuilder::GenerateFilter() {
  /// 1. 获取当前缓存的 key 数量，如果key数量为0，
  /// 将当前的 filter data 的偏移push到 filter offsets容器之后，就直接return
  const size_t num_keys = start_.size();
  if (num_keys == 0) {
    // Fast path if there are no keys for this filter
    filter_offsets_.push_back(result_.size());
    return;
  }

  
  // Make list of keys from flattened key structure

  /// 2. 将缓存的Flattened key contents 展开，add 到 tmp_keys_ 容器
  // 再push一个，避免start_[i + 1]时候溢出
  start_.push_back(keys_.size());  // Simplify length computation
  tmp_keys_.resize(num_keys);
  for (size_t i = 0; i < num_keys; i++) {
    const char* base = keys_.data() + start_[i];
    size_t length = start_[i + 1] - start_[i];
    tmp_keys_[i] = Slice(base, length);
  }

  /// 3. 将当前的SSTable的filter数据偏移添加到filter_offsets_容器中，
  /// 再将 tmp_keys_ （也就是当前缓存的key）生成filter Slice
  // Generate filter for current set of keys and append to result_.
  // 注意先push_back，再CreateFilter
  filter_offsets_.push_back(result_.size());
  policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result_);

  /// 4. clear status
  tmp_keys_.clear();
  keys_.clear();
  start_.clear();
}

FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,
                                     const Slice& contents)
    : policy_(policy), data_(nullptr), offset_(nullptr), num_(0), base_lg_(0) {
  size_t n = contents.size();
  if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array
  /// 1. 获取base值
  base_lg_ = contents[n - 1];

  // 2. 获取 filter offset 在filterBlock的偏移
  uint32_t last_word = DecodeFixed32(contents.data() + n - 5);
  if (last_word > n - 5) return;
  data_ = contents.data();
  offset_ = data_ + last_word;
  num_ = (n - 5 - last_word) / 4;
}

bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
  
  // 1. 获得其对应第几个 filter Slice 
  uint64_t index = block_offset >> base_lg_;
  if (index < num_) {
    // 2. 从filter index 中获取对应filter Slice 的位置。
    // start
    uint32_t start = DecodeFixed32(offset_ + index * 4);
    uint32_t limit = DecodeFixed32(offset_ + index * 4 + 4);
    if (start <= limit && limit <= static_cast<size_t>(offset_ - data_)) {
      // 3. 调用 policy_->KeyMayMatch 进行 bloom 过滤，并将结果返回
      Slice filter = Slice(data_ + start, limit - start);
      return policy_->KeyMayMatch(key, filter);
    } else if (start == limit) {
      // Empty filters do not match any keys
      return false;
    }
  }
  return true;  // Errors are treated as potential matches
}

}  // namespace leveldb
