// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A filter block is stored near the end of a Table file.  It contains
// filters (e.g., bloom filters) for all data blocks in the table combined
// into a single filter block.

#ifndef STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "leveldb/slice.h"
#include "util/hash.h"

namespace leveldb {

class FilterPolicy;

// A FilterBlockBuilder is used to construct all of the filters for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.
//
// The sequence of calls to FilterBlockBuilder must match the regexp:
//      (StartBlock AddKey*)* Finish
class FilterBlockBuilder {
 public:
  explicit FilterBlockBuilder(const FilterPolicy*);

  FilterBlockBuilder(const FilterBlockBuilder&) = delete;
  FilterBlockBuilder& operator=(const FilterBlockBuilder&) = delete;

  // 开始构建新的filter block，TableBuilder在构造函数和Flush中调用  
  void StartBlock(uint64_t block_offset);

  // 添加key，TableBuilder每次向data block中加入key时调用  
  void AddKey(const Slice& key);

  Slice Finish();

 private:
  // 将当前缓存的key生成新的filter Slice，然后清空当前key相关缓存。
  // 当调用 StartBlock 时，如果传入的 data block offset已经超过
  // 了可以生成filter Slice 的阈值时，调用该函数
  void GenerateFilter();

  // 所引用的过滤器
  const FilterPolicy* policy_;    

  // 扁平化后的keys缓存
  std::string keys_;             // Flattened key contents

  // 每次add key之前，将keys_.size()存入，可以推算每个缓存key的索引
  std::vector<size_t> start_;    // Starting index in keys_ of each key
  
  // 当前SSTable所生成的 filter 数据
  std::string result_;           // Filter data computed so far

  // 将 扁平的 keys_ 展开后临时存储到 tmp_keys_
  std::vector<Slice> tmp_keys_;  // policy_->CreateFilter() argument
  
  // 每次append result_ 之前，将result.size()存入，可以推算出每个filterSlice的索引
  std::vector<uint32_t> filter_offsets_;
};

class FilterBlockReader {
 public:
  // REQUIRES: "contents" and *policy must stay live while *this is live.
  
  // 传入Block content（没有 type || crc32 部分），将其解析成 FilterBlockReader
  FilterBlockReader(const FilterPolicy* policy, const Slice& contents);

  // 对Key进行匹配，执行 filter 职能
  bool KeyMayMatch(uint64_t block_offset, const Slice& key);

 private:
  // 所引用的过滤器
  const FilterPolicy* policy_;


  // 指向整个 filterBlock 的第一个元素
  const char* data_;    // Pointer to filter data (at block-start)
  
  // 指向offset数组的第一个元素
  const char* offset_;  // Pointer to beginning of offset array (at block-end)
  
  // offset 数组的元素个数
  size_t num_;          // Number of entries in offset array

  // FilterBlock 的阈值参数
  size_t base_lg_;      // Encoding parameter (see kFilterBaseLg in .cc file)
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
