// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_WRITE_BATCH_INTERNAL_H_
#define STORAGE_LEVELDB_DB_WRITE_BATCH_INTERNAL_H_

#include "db/dbformat.h"
#include "leveldb/write_batch.h"

namespace leveldb {

class MemTable;

// WriteBatchInternal provides static methods for manipulating a
// WriteBatch that we don't want in the public WriteBatch interface.
// 为 WriteBatch 提供的一些静态方法
class WriteBatchInternal {
 public:
  // Return the number of entries in the batch.
  // 返回 batch 中的entries数量
  static int Count(const WriteBatch* batch);

  // Set the count for the number of entries in the batch.
  // 设置 batch 中的 entries数量
  static void SetCount(WriteBatch* batch, int n);

  // Return the sequence number for the start of this batch.
  // 返回 batch 开端的 seq num
  static SequenceNumber Sequence(const WriteBatch* batch);

  // Store the specified number as the sequence number for the start of
  // this batch.
  // 为该 batch 开端设置 seq num
  static void SetSequence(WriteBatch* batch, SequenceNumber seq);

  // 返回 batch 的内容
  static Slice Contents(const WriteBatch* batch) { return Slice(batch->rep_); }

  // 返回 batch 的占用空间
  static size_t ByteSize(const WriteBatch* batch) { return batch->rep_.size(); }

  // 设置 batch 的内容为 contents
  static void SetContents(WriteBatch* batch, const Slice& contents);

  /**
   * @brief 将 batch 的内容 apply 到 memtable 中
   * 
   * @param batch 
   * @param memtable 
   * @return Status 
   */
  static Status InsertInto(const WriteBatch* batch, MemTable* memtable);

  /**
   * @brief 将 src 中的 op append 到 dst 中
   * 
   * @param dst 目标 Batch
   * @param src 源 Batch
   */
  static void Append(WriteBatch* dst, const WriteBatch* src);
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_WRITE_BATCH_INTERNAL_H_
