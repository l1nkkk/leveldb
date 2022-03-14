// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/two_level_iterator.h"

#include "leveldb/table.h"
#include "table/block.h"
#include "table/format.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

namespace {

typedef Iterator* (*BlockFunction)(void*, const ReadOptions&, const Slice&);

// 专门为Table定制的
class TwoLevelIterator : public Iterator {
 public:
  TwoLevelIterator(Iterator* index_iter, BlockFunction block_function,
                   void* arg, const ReadOptions& options);

  ~TwoLevelIterator() override;

  void Seek(const Slice& target) override;
  void SeekToFirst() override;
  void SeekToLast() override;
  void Next() override;
  void Prev() override;

  bool Valid() const override { return data_iter_.Valid(); }
  Slice key() const override {
    assert(Valid());
    return data_iter_.key();
  }
  Slice value() const override {
    assert(Valid());
    return data_iter_.value();
  }
  Status status() const override {
    // It'd be nice if status() returned a const Status& instead of a Status
    if (!index_iter_.status().ok()) {
      return index_iter_.status();
    } else if (data_iter_.iter() != nullptr && !data_iter_.status().ok()) {
      return data_iter_.status();
    } else {
      return status_;
    }
  }

 private:
  void SaveError(const Status& s) {
    if (status_.ok() && !s.ok()) status_ = s;
  }

  // 如果data_iter 已经指向该 block 的末尾，切换到下一个 block,
  // if data_iter_.iter() == nullptr || !data_iter_.Valid(),
  // index_iter_.Next() 的方式切换data Block
  void SkipEmptyDataBlocksForward();

  // 如果data_iter 已经指向该 block 的末尾，切换到上一个 block，
  // if data_iter_.iter() == nullptr || !data_iter_.Valid(),
  // index_iter_.Prev() 的方式切换data Block
  void SkipEmptyDataBlocksBackward();

  // 设置当前 data block 的iterator
  void SetDataIterator(Iterator* data_iter);

  // 根据当前的index block 迭代器index_iter_，
  // 更新data block 迭代器 data_iter_的位置
  void InitDataBlock();

  BlockFunction block_function_;  // block解析函数  
  void* arg_;                     // BlockFunction 的参数，一般传入Table*
  const ReadOptions options_;     // BlockFunction 的 ReadOptions 实参
  Status status_;                 // iter 状态
  IteratorWrapper index_iter_;    // index Block 的迭代器
  IteratorWrapper data_iter_;     // May be nullptr，current data Block iterator
  // If data_iter_ is non-null, then "data_block_handle_" holds the
  // "index_value" passed to block_function_ to create the data_iter_.
  // 如果 data_iter_ 不为 null，说明调用过 block_function_ 进行创建，
  // 此时 data_block_handle_ 记录的是当前 data Block 的 BlockHandle，
  // 避免重复Seek的适合，相同的 BlockHandle 创建多次。
  std::string data_block_handle_;
};

TwoLevelIterator::TwoLevelIterator(Iterator* index_iter,
                                   BlockFunction block_function, void* arg,
                                   const ReadOptions& options)
    : block_function_(block_function),
      arg_(arg),
      options_(options),
      index_iter_(index_iter),
      data_iter_(nullptr) {}

TwoLevelIterator::~TwoLevelIterator() = default;

// 注：想想 index Block 中的 key 是什么，那样设计的作用体现了。
void TwoLevelIterator::Seek(const Slice& target) {
  // 1. 先 seek index iterator
  index_iter_.Seek(target);

  // 2. 再调整当前 data block 迭代器指向的 block
  InitDataBlock();

  // 3. 再定位 当前 data block 迭代器 的位置
  if (data_iter_.iter() != nullptr) data_iter_.Seek(target);

  // 4. 如果data_iter 已经指向该 block 的末尾，切换到下一个 block
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToFirst() {
  index_iter_.SeekToFirst();
  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToLast() {
  index_iter_.SeekToLast();
  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::Next() {
  assert(Valid());
  data_iter_.Next();
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::Prev() {
  assert(Valid());
  data_iter_.Prev();
  SkipEmptyDataBlocksBackward();
}

void TwoLevelIterator::SkipEmptyDataBlocksForward() {
  while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(nullptr);
      return;
    }
    index_iter_.Next();
    InitDataBlock();
    if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
  }
}

// 
void TwoLevelIterator::SkipEmptyDataBlocksBackward() {
  while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(nullptr);
      return;
    }
    index_iter_.Prev();
    InitDataBlock();
    if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
  }
}

void TwoLevelIterator::SetDataIterator(Iterator* data_iter) {
  if (data_iter_.iter() != nullptr) SaveError(data_iter_.status());
  data_iter_.Set(data_iter);
}

void TwoLevelIterator::InitDataBlock() {
  if (!index_iter_.Valid()) {
    SetDataIterator(nullptr);
  } else {
    // 1. 从index_iter_，获取 data block 的BlockHandle
    Slice handle = index_iter_.value();
    if (data_iter_.iter() != nullptr &&
        handle.compare(data_block_handle_) == 0) {
      // 当前 data_iter_ 所迭代的块，就是所要切换的块
      // data_iter_ is already constructed with this iterator, so
      // no need to change anything
    } else {
      // 2. 导入data block数据，返回迭代器
      Iterator* iter = (*block_function_)(arg_, options_, handle);
      data_block_handle_.assign(handle.data(), handle.size());
      SetDataIterator(iter);
    }
  }
}

}  // namespace

Iterator* NewTwoLevelIterator(Iterator* index_iter,
                              BlockFunction block_function, void* arg,
                              const ReadOptions& options) {
  return new TwoLevelIterator(index_iter, block_function, arg, options);
}

}  // namespace leveldb
