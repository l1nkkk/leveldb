// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// The representation of a DBImpl consists of a set of Versions.  The
// newest version is called "current".  Older versions may be kept
// around to provide a consistent view to live iterators.
//
// Each Version keeps track of a set of Table files per level.  The
// entire set of versions is maintained in a VersionSet.
//
// Version,VersionSet are thread-compatible, but require external
// synchronization on all accesses.

#ifndef STORAGE_LEVELDB_DB_VERSION_SET_H_
#define STORAGE_LEVELDB_DB_VERSION_SET_H_

#include "db/dbformat.h"
#include "db/version_edit.h"
#include <map>
#include <set>
#include <vector>

#include "port/port.h"
#include "port/thread_annotations.h"

namespace leveldb {

namespace log {
class Writer;
}

class Compaction;
class Iterator;
class MemTable;
class TableBuilder;
class TableCache;
class Version;
class VersionSet;
class WritableFile;

// Return the smallest index i such that files[i]->largest >= key.
// Return files.size() if there is no such file.
// REQUIRES: "files" contains a sorted list of non-overlapping files.
// 返回最小的索引i，使得 files[i]->largest >= key，
// 如果没找到，返回 files.size()
// 要求：files 包含一系列key有序且不覆盖的文件元信息
int FindFile(const InternalKeyComparator& icmp,
             const std::vector<FileMetaData*>& files, const Slice& key);

// Returns true iff some file in "files" overlaps the user key range
// [*smallest,*largest].
// smallest==nullptr represents a key smaller than all keys in the DB.
// largest==nullptr represents a key largest than all keys in the DB.
// REQUIRES: If disjoint_sorted_files, files[] contains disjoint ranges
//           in sorted order.
bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                           bool disjoint_sorted_files,
                           const std::vector<FileMetaData*>& files,
                           const Slice* smallest_user_key,
                           const Slice* largest_user_key);

// 一个Version就是一个某个特定版本的sstable文件集合，以及它管理的compact状态，
// Version不会修改其管理的sstable文件，只有读取操作，多个并且构成了一个双向循环链表
class Version {
 public:
  // GetStats 包含两个数据，分别是文件元数据和其所在level
  struct GetStats {
    FileMetaData* seek_file;
    int seek_file_level;
  };

  // Append to *iters a sequence of iterators that will
  // yield the contents of this Version when merged together.
  // REQUIRES: This version has been saved (see VersionSet::SaveTo)
  // 向 *iters 追加一系列迭代器，这些迭代器在合并在一起时将产生此版本的内容。
  // 该接口为了遍历该版本下的所有SSTable。
  // 要求： 这个版本已经被 saved
  void AddIterators(const ReadOptions&, std::vector<Iterator*>* iters);

  // Lookup the value for key.  If found, store it in *val and
  // return OK.  Else return a non-OK status.  Fills *stats.
  // REQUIRES: lock is not held
  // 获取对应 key 的 val，如果找到，将其值设置到 $val 当中作为输出，并返回OK；
  // 否则返回 non-OK 状态；
  // 要求: lock is not held(没被上锁)
  Status Get(const ReadOptions&, const LookupKey& key, std::string* val,
             GetStats* stats);

  // Adds "stats" into the current state.  Returns true if a new
  // compaction may need to be triggered, false otherwise.
  // REQUIRES: lock is held
  // 对 FileMetaData 中的 allowed_seeks 进行递减，递减为0了之后，
  // 设置该Version的 seek compaction
  // 机制的参数，并返回true，表示需要触发compaction 要求：lock is held
  bool UpdateStats(const GetStats& stats);

  // Record a sample of bytes read at the specified internal key.
  // Samples are taken approximately once every config::kReadBytesPeriod
  // bytes.  Returns true if a new compaction may need to be triggered.
  // REQUIRES: lock is held
  // 记录从指定的internalkey $key中读取到的字节样本，
  // 大约每 config::kReadBytesPeriod 字节采样一次,
  // 如果可能需要触发新的压缩，则返回 true。
  // REQUIRES: lock is held
  bool RecordReadSample(Slice key);

  // Reference count management (so Versions do not disappear out from
  // under live iterators)
  // 引用计数管理
  void Ref();
  void Unref();

  // Store in "*inputs" all files in "level" that overlap [begin,end]
  // 在指定的level中，找到和 [begin, end] 有重合的 fileMetaData
  void GetOverlappingInputs(
      int level,
      const InternalKey* begin,  // nullptr means before all keys
      const InternalKey* end,    // nullptr means after all keys
      std::vector<FileMetaData*>* inputs);

  // Returns true iff some file in the specified level overlaps
  // some part of [*smallest_user_key,*largest_user_key].
  // smallest_user_key==nullptr represents a key smaller than all the DB's keys.
  // largest_user_key==nullptr represents a key largest than all the DB's keys.
  // 如果指定 $level 的某些文件和 [*smallest_user_key,*largest_user_key]
  // 有重复， 则返回 true；smallest_user_key==nullptr 表示
  // 一个比DB中所有的key还小的key， largest_user_key==nullptr 表示
  // 一个比DB中所有的key还大的key
  bool OverlapInLevel(int level, const Slice* smallest_user_key,
                      const Slice* largest_user_key);

  // Return the level at which we should place a new memtable compaction
  // result that covers the range [smallest_user_key,largest_user_key].
  // 给定memtable中key的范围 [smallest_user_key,largest_user_key]，
  // 返回其应该被落盘到哪个level
  int PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                 const Slice& largest_user_key);

  // 返回该版本下，指定level的sstable文件个数
  int NumFiles(int level) const { return files_[level].size(); }

  // Return a human readable string that describes this version's contents.
  std::string DebugString() const;

 private:
  friend class Compaction;
  friend class VersionSet;

  class LevelFileNumIterator;

  explicit Version(VersionSet* vset)
      : vset_(vset),
        next_(this),
        prev_(this),
        refs_(0),
        file_to_compact_(nullptr),
        file_to_compact_level_(-1),
        compaction_score_(-1),
        compaction_level_(-1) {}

  Version(const Version&) = delete;
  Version& operator=(const Version&) = delete;

  ~Version();

  // 返回一个TwoLevelIterator对象，用来遍历<version,level >中sstable的内容
  Iterator* NewConcatenatingIterator(const ReadOptions&, int level) const;

  // Call func(arg, level, f) for every file that overlaps user_key in
  // order from newest to oldest.  If an invocation of func returns
  // false, makes no more calls.
  //
  // REQUIRES: user portion of internal_key == user_key.
  // 从最新到最旧检索 fileMetaData ，使得 fileMetaData 范围覆盖了 user_key，
  // 对检索到的fileMetaData 调用func(arg, level, f)，如果 func
  // 返回fasle，则return，不再继续检索
  void ForEachOverlapping(Slice user_key, Slice internal_key, void* arg,
                          bool (*func)(void*, int, FileMetaData*));

  // 构成Version双向循环链表，vset_中存在链表表头指针
  VersionSet* vset_;  // VersionSet to which this Version belongs
  Version* next_;     // Next version in linked list
  Version* prev_;     // Previous version in linked list
  int refs_;          // Number of live refs to this version

  // List of files per level
  // sstable文件列表，files_[0] 第0层的sstable
  std::vector<FileMetaData*> files_[config::kNumLevels];

  // ==Seek触发Compaction
  // Next file to compact based on seek stats.
  // 下一个要compact的文件
  FileMetaData* file_to_compact_;
  int file_to_compact_level_;

  /// ==容量触发Compaction
  // Level that should be compacted next and its compaction score.
  // Score < 1 means compaction is not strictly needed.  These fields
  // are initialized by Finalize().
  // 下一个应该compact的level和compaction分数.
  // 分数 < 1 说明compaction并不紧迫. 这些字段在Finalize()中初始化
  double compaction_score_;
  int compaction_level_;
};

class VersionSet {
 public:
  // VersionSet会使用到TableCache，这个是调用者传入的。TableCache用于Get k/v操作
  VersionSet(const std::string& dbname, const Options* options,
             TableCache* table_cache, const InternalKeyComparator*);
  VersionSet(const VersionSet&) = delete;
  VersionSet& operator=(const VersionSet&) = delete;

  ~VersionSet();

  // Apply *edit to the current version to form a new descriptor that
  // is both saved to persistent state and installed as the new
  // current version.  Will release *mu while actually writing to the file.
  // REQUIRES: *mu is held on entry.
  // REQUIRES: no other thread concurrently calls LogAndApply()
  // 在current version上 apply 指定的VersionEdit；如果当前没有MANIFEST文件，
  // 则创建MANIFEST文件并写入全量版本信息
  // curentVersion；如果当前存在NANIFEST文件， 则将 增量版本信息 edit
  // 追加到文件中
  Status LogAndApply(VersionEdit* edit, port::Mutex* mu)
      EXCLUSIVE_LOCKS_REQUIRED(mu);

  // Recover the last saved descriptor from persistent storage.
  // 从磁盘恢复最后持久化的元数据信息，save_manifest
  // 返回是否需要重新写一个新的MANIFEST
  Status Recover(bool* save_manifest);

  // Return the current version.
  // 获取 current_
  Version* current() const { return current_; }

  // Return the current manifest file number
  // 获取 manifest_file_number_
  uint64_t ManifestFileNumber() const { return manifest_file_number_; }

  // Allocate and return a new file number
  // 分配一个fileNum；获取next_file_number_，之后next_file_number_++。
  uint64_t NewFileNumber() { return next_file_number_++; }

  // Arrange to reuse "file_number" unless a newer file number has
  // already been allocated.
  // REQUIRES: "file_number" was returned by a call to NewFileNumber().
  //  重用file_number，必须满足 next_file_number_ == file_number + 1
  void ReuseFileNumber(uint64_t file_number) {
    if (next_file_number_ == file_number + 1) {
      next_file_number_ = file_number;
    }
  }

  // Return the number of Table files at the specified level.
  // 返回指定level的文件个数
  int NumLevelFiles(int level) const;

  // Return the combined file size of all files at the specified level.
  // 返回指定level的所有文件大小和
  int64_t NumLevelBytes(int level) const;

  // Return the last sequence number.
  // 获取 last_sequence_
  uint64_t LastSequence() const { return last_sequence_; }

  // Set the last sequence number to s.
  // 设置 last_sequence_
  void SetLastSequence(uint64_t s) {
    assert(s >= last_sequence_);
    last_sequence_ = s;
  }

  // Mark the specified file number as used.
  // 标记指定的文件编号已经被使用了，内部更新 next_file_number_
  void MarkFileNumberUsed(uint64_t number);

  // Return the current log file number.
  // 返回 log_number_
  uint64_t LogNumber() const { return log_number_; }

  // Return the log file number for the log file that is currently
  // being compacted, or zero if there is no such log file.
  // 返回正在compact的log文件编号，如果没有返回0
  uint64_t PrevLogNumber() const { return prev_log_number_; }

  // Pick level and inputs for a new compaction.
  // Returns nullptr if there is no compaction to be done.
  // Otherwise returns a pointer to a heap-allocated object that
  // describes the compaction.  Caller should delete the result.
  //
  Compaction* PickCompaction();

  // Return a compaction object for compacting the range [begin,end] in
  // the specified level.  Returns nullptr if there is nothing in that
  // level that overlaps the specified range.  Caller should delete
  // the result.
  /**
   * @brief 返回针对指定层数的某个范围的文件进行compact的 Compaction对象，
   * 如果 begin，end
   * 在当前层数没有overlap到，则返回nullptr，调用者应该负责delete该返回结果
   *
   * @param level 指定层数
   * @param begin 范围
   * @param end   范围
   * @return Compaction*
   */
  Compaction* CompactRange(int level, const InternalKey* begin,
                           const InternalKey* end);

  // Return the maximum overlapping data (in bytes) at next level for any
  // file at a level >= 1.
  // 对于所有level>0，遍历文件，找到和下一层文件的重叠数据的最大值(in bytes)
  // 这个就是Version:: GetOverlappingInputs()函数的简单应用
  int64_t MaxNextLevelOverlappingBytes();

  // Create an iterator that reads over the compaction inputs for "*c".
  // The caller should delete the iterator when no longer needed.
  Iterator* MakeInputIterator(Compaction* c);

  // Returns true iff some level needs a compaction.
  // 返回是否需要触发 major compact，在Seek compact 和 size compact 机制上
  bool NeedsCompaction() const {
    Version* v = current_;
    return (v->compaction_score_ >= 1) || (v->file_to_compact_ != nullptr);
  }

  // Add all files listed in any live version to *live.
  // May also mutate some internal state.
  // 返回目前所有活跃着的fileNum，只要是VersionSet中的Version所包含的文件，都是alive的
  void AddLiveFiles(std::set<uint64_t>* live);

  // Return the approximate offset in the database of the data for
  // "key" as of version "v".
  // 在特定的某个version v中，返回key对应的一个大概的offset
  uint64_t ApproximateOffsetOf(Version* v, const InternalKey& key);

  // Return a human-readable short (single-line) summary of the number
  // of files per level.  Uses *scratch as backing store.
  struct LevelSummaryStorage {
    char buffer[100];
  };
  const char* LevelSummary(LevelSummaryStorage* scratch) const;

 private:
  class Builder;

  friend class Compaction;
  friend class Version;

  // 返回是否继续使用当前的 Manifest 文件，可能存在 Manifest
  // 文件过大，需要更换的情况; 如果可以重用，内部设置 descriptor_log_ 和
  // manifest_file_number_
  bool ReuseManifest(const std::string& dscname, const std::string& dscbase);

  // 对于当前版本 v，计算最佳的压缩 level，并计算一个 compaction 分数，
  // 最终设置 $v 的 compaction_score_ 和  compaction_level_
  void Finalize(Version* v);

  void GetRange(const std::vector<FileMetaData*>& inputs, InternalKey* smallest,
                InternalKey* largest);

  void GetRange2(const std::vector<FileMetaData*>& inputs1,
                 const std::vector<FileMetaData*>& inputs2,
                 InternalKey* smallest, InternalKey* largest);

  void SetupOtherInputs(Compaction* c);

  // Save current contents to *log
  // 把current Version(全量版本信息)保存到*log中，
  // 信息包括comparator名字、compaction点和各级sstable文件
  Status WriteSnapshot(log::Writer* log);

  // 将version v 放入双向链表中，并将 current_ 指向 v
  void AppendVersion(Version* v);

  //=== 第一组，直接来自于DBImple，构造函数传入
  Env* const env_;                 // 操作系统封装
  const std::string dbname_;       // 数据库名称，即数据库所在路径
  const Options* const options_;   // 配置选项
  TableCache* const table_cache_;  // sstable cache
  const InternalKeyComparator icmp_;  // internalKey cmp

  //=== 第二组，db元信息相关
  uint64_t next_file_number_;      // 下一个op log文件编号
  uint64_t manifest_file_number_;  // 当前的manifest文件编号
  uint64_t last_sequence_;         // 当前的seq，用于构造 internalKey
  uint64_t log_number_;            // log编号
  uint64_t prev_log_number_;  // 前一个op log文件编号，0 or backing store for
                              // memtable being compacted

  // Opened lazily
  //=== 第三组，menifest文件相关
  WritableFile* descriptor_file_;
  log::Writer* descriptor_log_;

  //=== 第四组，版本管理
  // versions双向链表头
  Version dummy_versions_;  // Head of circular doubly-linked list of versions.
  Version* current_;        // == dummy_versions_.prev_

  // Per-level key at which the next compaction at that level should start.
  // Either an empty string, or a valid InternalKey.
  // 每个level下一次compaction的开始key，其值为空字符串或者合法的InternalKey
  std::string compact_pointer_[config::kNumLevels];
};

// A Compaction encapsulates information about a compaction.
class Compaction {
 public:
  ~Compaction();

  // Return the level that is being compacted.  Inputs from "level"
  // and "level+1" will be merged to produce a set of "level+1" files.
  // 返回待compact的level
  int level() const { return level_; }

  // Return the object that holds the edits to the descriptor done
  // by this compaction.
  // 返回本次 compact 产生的版本增量
  VersionEdit* edit() { return &edit_; }

  // "which" must be either 0 or 1
  // 返回待compact的文件数，0表示level_，1表示level_+1
  int num_input_files(int which) const { return inputs_[which].size(); }

  // Return the ith input file at "level()+which" ("which" must be 0 or 1).
  // 返回待compact的某个文件的FileMetaData，return inputs_[which][i]
  FileMetaData* input(int which, int i) const { return inputs_[which][i]; }

  // Maximum size of files to build during this compaction.
  // compact 输出文件所在层次所允许的单个文件最大大小
  uint64_t MaxOutputFileSize() const { return max_output_file_size_; }

  // Is this a trivial compaction that can be implemented by just
  // moving a single input file to the next level (no merging or splitting)
  // 返回是不是 trivial compaction
  bool IsTrivialMove() const;

  // Add all inputs to this compaction as delete operations to *edit.
  // 将compact的文件信息记录到 edit的删除列表中
  void AddInputDeletions(VersionEdit* edit);

  // Returns true if the information we have available guarantees that
  // the compaction is producing data in "level+1" for which no data exists
  // in levels greater than "level+1".
  // 如果确定compact后文件落盘到level_+1，该函数判断在level_+2及其以后的层里，
  // 是否存在overlap $userKey 的sstable文件
  bool IsBaseLevelForKey(const Slice& user_key);

  // Returns true iff we should stop building the current output
  // before processing "internal_key".
  bool ShouldStopBefore(const Slice& internal_key);

  // Release the input version for the compaction, once the compaction
  // is successful.
  // 一旦 compaction 成功， release input version
  void ReleaseInputs();

 private:
  friend class Version;
  friend class VersionSet;

  Compaction(const Options* options, int level);

  int level_;  // 待compact层次
  uint64_t max_output_file_size_;  // compact 输出文件所在层次所允许的单个文件最大大小
  Version* input_version_;  // 待 compact 的Version
  VersionEdit edit_;        // compact 产生的版本增量

  // Each compaction reads inputs from "level_" and "level_+1"
  // level_和level_+1中需要compact的 sst 文件对应的 FileMetaData
  std::vector<FileMetaData*> inputs_[2];  // The two sets of inputs

  // State used to check for number of overlapping grandparent files
  // (parent == level_ + 1, grandparent == level_ + 2)
  // 在VersionSet::SetupOtherInputs中被设置，用于 check 和grandparent overlap
  // 的文件数量，太大的话怎么办?
  std::vector<FileMetaData*> grandparents_;

  // 用于 ShouldStopBefore 中
  size_t grandparent_index_;  // Index in grandparent_starts_
  bool seen_key_;             // Some output key has been seen
  int64_t overlapped_bytes_;  // Bytes of overlap between current output
                              // and grandparent files

  // State for implementing IsBaseLevelForKey

  // level_ptrs_ holds indices into input_version_->levels_: our state
  // is that we are positioned at one of the file ranges for each
  // higher level than the ones involved in this compaction (i.e. for
  // all L >= level_ + 2).
  // 辅助 IsBaseLevelForKey 的实现
  size_t level_ptrs_[config::kNumLevels];
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_VERSION_SET_H_
