// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

#include "db/filename.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/table_cache.h"
#include <algorithm>
#include <cstdio>

#include "leveldb/env.h"
#include "leveldb/table_builder.h"

#include "table/merger.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/logging.h"

namespace leveldb {

// 返回 options->max_file_size ，默认为2MB
static size_t TargetFileSize(const Options* options) {
  return options->max_file_size;
}

// Maximum bytes of overlaps in grandparent (i.e., level+2) before we
// stop building a single file in a level->level+1 compaction.
// 返回与Level+2之间overlap允许的最大文件大小，默认为 10 * 2MB
static int64_t MaxGrandParentOverlapBytes(const Options* options) {
  return 10 * TargetFileSize(options);  // 最大文件大小（2MB）* 10
}

// Maximum number of bytes in all compacted files.  We avoid expanding
// the lower level file set of a compaction if it would make the
// total compaction cover more than this many bytes.
// 需要 compact 文件大小的上界，默认为 25 * 2MB
static int64_t ExpandedCompactionByteSizeLimit(const Options* options) {
  return 25 * TargetFileSize(options);
}

// 返回某一层允许的总文件大小
static double MaxBytesForLevel(const Options* options, int level) {
  // Note: the result for level zero is not really used since we set
  // the level-0 compaction threshold based on number of files.

  // Result for both level-0 and level-1
  double result = 10. * 1048576.0;
  while (level > 1) {
    result *= 10;
    level--;
  }
  return result;
}


// 返回某一层允许的单个文件最大字节数
static uint64_t MaxFileSizeForLevel(const Options* options, int level) {
  // We could vary per level to reduce number of files?
  return TargetFileSize(options);
}

// 返回 传入的fileMetaData集合所记录的文件的 总文件大小
static int64_t TotalFileSize(const std::vector<FileMetaData*>& files) {
  int64_t sum = 0;
  for (size_t i = 0; i < files.size(); i++) {
    sum += files[i]->file_size;
  }
  return sum;
}

Version::~Version() {
  assert(refs_ == 0);

  // Remove from linked list
  prev_->next_ = next_;
  next_->prev_ = prev_;

  // Drop references to files
  for (int level = 0; level < config::kNumLevels; level++) {
    for (size_t i = 0; i < files_[level].size(); i++) {
      FileMetaData* f = files_[level][i];
      assert(f->refs > 0);
      f->refs--;
      if (f->refs <= 0) {
        delete f;
      }
    }
  }
}

// 通过二分法，找到overlap $key 的$files索引
int FindFile(const InternalKeyComparator& icmp,
             const std::vector<FileMetaData*>& files, const Slice& key) {
  uint32_t left = 0;
  uint32_t right = files.size();
  // 二分查找
  while (left < right) {
    uint32_t mid = (left + right) / 2;
    const FileMetaData* f = files[mid];
    if (icmp.InternalKeyComparator::Compare(f->largest.Encode(), key) < 0) {
      // Key at "mid.largest" is < "target".  Therefore all
      // files at or before "mid" are uninteresting.
      left = mid + 1;
    } else {
      // Key at "mid.largest" is >= "target".  Therefore all files
      // after "mid" are uninteresting.
      right = mid;
    }
  }
  return right;
}


// 是否 f->smallest.user_key() ... f->largest.user_key() ... user_key;
// 如果是，返回true         
static bool AfterFile(const Comparator* ucmp, const Slice* user_key,
                      const FileMetaData* f) {
  // null user_key occurs before all keys and is therefore never after *f
  return (user_key != nullptr &&
          ucmp->Compare(*user_key, f->largest.user_key()) > 0);
}

// 是否 user_key ... f->smallest.user_key() ... f->largest.user_key();
// 如果是，返回false
static bool BeforeFile(const Comparator* ucmp, const Slice* user_key,
                       const FileMetaData* f) {
  // null user_key occurs after all keys and is therefore never before *f
  return (user_key != nullptr &&
          ucmp->Compare(*user_key, f->smallest.user_key()) < 0);
}


// 如果overlap返回true；
// 返回 files[] 中的 key range 是否overlap [smallest_user_key, largest_user_key],
// 如果是，返回true；
bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                           bool disjoint_sorted_files,
                           const std::vector<FileMetaData*>& files,
                           const Slice* smallest_user_key,
                           const Slice* largest_user_key) {

  // disjoint_sorted_files == true，表明level >= 1，说明各个 fileMetaData 的
  // key range 不相交，且有序
  const Comparator* ucmp = icmp.user_comparator();

  /// 1. 处理 level-0的情况，整个遍历，有覆盖则返回true，没有返回false
  if (!disjoint_sorted_files) {
    // Need to check against all files
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      if (AfterFile(ucmp, smallest_user_key, f) ||
          BeforeFile(ucmp, largest_user_key, f)) {
        // No overlap
      } else {
        return true;  // Overlap
      }
    }
    return false;
  }

  /// 2. 处理 level >= 1的情况，直接二分查找
  // Binary search over file list
  uint32_t index = 0;
  if (smallest_user_key != nullptr) {
    // Find the earliest possible internal key for smallest_user_key
    InternalKey small_key(*smallest_user_key, kMaxSequenceNumber,
                          kValueTypeForSeek);
    index = FindFile(icmp, files, small_key.Encode());
  }

  if (index >= files.size()) { // 不存在比smallest_user_key小的key
    // beginning of range is after all files, so no overlap.
    return false;
  }

  // 保证在largest_user_key之后
  return !BeforeFile(ucmp, largest_user_key, files[index]);
}




// An internal iterator.  For a given version/level pair, yields
// information about the files in the level.  For a given entry, key()
// is the largest key that occurs in the file, and value() is an
// 16-byte value containing the file number and file size, both
// encoded using EncodeFixed64.
// 一个内部的迭代器，迭代给定 version/level 对应的相关文件信息。
// Key() 返回的是当前文件的最大key值，
// Value() 返回一格16B的数据，里面存储 file number 和 file size
class Version::LevelFileNumIterator : public Iterator {
 public:
  // @icmp: 比较器
  // @flist: 某一层的所有 table 文件的元数据
  LevelFileNumIterator(const InternalKeyComparator& icmp,
                       const std::vector<FileMetaData*>* flist)
      : icmp_(icmp), flist_(flist), index_(flist->size()) {  // Marks as invalid
  }
  bool Valid() const override { return index_ < flist_->size(); }
  void Seek(const Slice& target) override {
    index_ = FindFile(icmp_, *flist_, target);
  }
  void SeekToFirst() override { index_ = 0; }
  void SeekToLast() override {
    index_ = flist_->empty() ? 0 : flist_->size() - 1;
  }
  void Next() override {
    assert(Valid());
    index_++;
  }
  void Prev() override {
    assert(Valid());
    if (index_ == 0) {
      index_ = flist_->size();  // Marks as invalid
    } else {
      index_--;
    }
  }

  // 当前文件元数据的最大key
  Slice key() const override {
    assert(Valid());
    return (*flist_)[index_]->largest.Encode();
  }

  // file number and file size 的封装
  Slice value() const override {
    assert(Valid());
    EncodeFixed64(value_buf_, (*flist_)[index_]->number);
    EncodeFixed64(value_buf_ + 8, (*flist_)[index_]->file_size);
    return Slice(value_buf_, sizeof(value_buf_));
  }
  Status status() const override { return Status::OK(); }

 private:
  // indexKey 比较器
  const InternalKeyComparator icmp_;
  const std::vector<FileMetaData*>* const flist_;

  // 当前索引
  uint32_t index_;

  // Backing store for value().  Holds the file number and size.
  // 存储当前迭代器的 value
  mutable char value_buf_[16];
};

/// 直接返回TableCache::NewIterator()，将迭代器指向的数据放在缓存
static Iterator* GetFileIterator(void* arg, const ReadOptions& options,
                                 const Slice& file_value) {
  TableCache* cache = reinterpret_cast<TableCache*>(arg);
  if (file_value.size() != 16) {  // 错误
    return NewErrorIterator(
        Status::Corruption("FileReader invoked with unexpected value"));
  } else {
    /// 解析 file_value，将读取的SSTable对象 dump到TableCache中，并返回其迭代器
    return cache->NewIterator(options, DecodeFixed64(file_value.data()),
                              DecodeFixed64(file_value.data() + 8));
  }
}

/// 返回一个TwoLevelIterator对象
Iterator* Version::NewConcatenatingIterator(const ReadOptions& options,
                                            int level) const {
  return NewTwoLevelIterator(
      new LevelFileNumIterator(vset_->icmp_, &files_[level]), &GetFileIterator,
      vset_->table_cache_, options);
}

void Version::AddIterators(const ReadOptions& options,
                           std::vector<Iterator*>* iters) {
  // Merge all level zero files together since they may overlap
  /// 1.
  /// 对于level=0级别的sstable文件，直接装入cache，level0的sstable文件可能有重合，需要merge
  for (size_t i = 0; i < files_[0].size(); i++) {
    iters->push_back(vset_->table_cache_->NewIterator(
        options, files_[0][i]->number, files_[0][i]->file_size));
  }

  // For levels > 0, we can use a concatenating iterator that sequentially
  // walks through the non-overlapping files in the level, opening them
  // lazily.
  /// 2. 对于level>0级别的sstable文件，lazy open机制，它们不会有重叠
  for (int level = 1; level < config::kNumLevels; level++) {
    if (!files_[level].empty()) {
      iters->push_back(NewConcatenatingIterator(options, level));
    }
  }
}

// Callback from TableCache::Get()
namespace {
enum SaverState {
  kNotFound,
  kFound,
  kDeleted,
  kCorrupt,
};

// Saver 封装所要检索的数据，包含四个变量，
// 分别是状态(kNotFound、kDeleted...)、比较器、user_key、value
struct Saver {
  SaverState state;
  const Comparator* ucmp;
  Slice user_key;
  std::string* value;
};
}  // namespace

/// 用于缓存Get数据时的回调，将获取到的数据封装成Saver
static void SaveValue(void* arg, const Slice& ikey, const Slice& v) {
  Saver* s = reinterpret_cast<Saver*>(arg);
  ParsedInternalKey parsed_key;
  if (!ParseInternalKey(ikey, &parsed_key)) {
    s->state = kCorrupt;
  } else {
    if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0) {
      s->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
      if (s->state == kFound) {
        s->value->assign(v.data(), v.size());
      }
    }
  }
}

static bool NewestFirst(FileMetaData* a, FileMetaData* b) {
  return a->number > b->number;
}

void Version::ForEachOverlapping(Slice user_key, Slice internal_key, void* arg,
                                 bool (*func)(void*, int, FileMetaData*)) {
  const Comparator* ucmp = vset_->icmp_.user_comparator();

  // Search level-0 in order from newest to oldest.

  /// 1. 遍历level-0，找到overlap @user_key 的 FileMetaData
  std::vector<FileMetaData*> tmp;
  tmp.reserve(files_[0].size());
  for (uint32_t i = 0; i < files_[0].size(); i++) {
    FileMetaData* f = files_[0][i];
    if (ucmp->Compare(user_key, f->smallest.user_key()) >= 0 &&
        ucmp->Compare(user_key, f->largest.user_key()) <= 0) {
      tmp.push_back(f);
    }
  }
  if (!tmp.empty()) {
    // 1-1. 将找到的一系列 level-0 FileMetaDatas 按 filenum从大到小排序（从新到旧）
    std::sort(tmp.begin(), tmp.end(), NewestFirst);
    // 1-2. match key，判断是否继续往下查找
    for (uint32_t i = 0; i < tmp.size(); i++) {
      if (!(*func)(arg, 0, tmp[i])) {
        return;
      }
    }
  }
  // Search other levels.
  /// 2. 如果 level 0 中没找到，二分检索 level >= 1 的 fileMetaData，
  /// 找到overlap @user_key 的 FileMetaData，如果某一层的结果成功让 $func 返回false，则return
  for (int level = 1; level < config::kNumLevels; level++) {
    size_t num_files = files_[level].size();
    if (num_files == 0) continue;

    // Binary search to find earliest index whose largest key >= internal_key.
    /// 2-1 因为其有序，且key不会重复。所以可以用二分法
    uint32_t index = FindFile(vset_->icmp_, files_[level], internal_key);
    if (index < num_files) {
      FileMetaData* f = files_[level][index];
      if (ucmp->Compare(user_key, f->smallest.user_key()) < 0) {
        // All of "f" is past any data for user_key
        // 说明要检索的 user_key，落在了该层sstable key range 的gap中
      } else {
        if (!(*func)(arg, level, f)) {
          return;
        }
      }
    }
  }
}

/**
 * @brief 获取对应 key 的 val，如果找到，将其值设置到 value 当中作为输出，并返回OK，否则返回 non-OK 状态；
 * 
 * @param options 
 * @param k in, 待检索的key
 * @param value out, 检索到的value
 * @param stats out，seek compaction 机制，如果被设置，那么调用 UpdateStats 进行状态更新
 * @return Status state.found ? state.s : Status::NotFound(Slice());
 */
Status Version::Get(const ReadOptions& options, const LookupKey& k,
                    std::string* value, GetStats* stats) {
  stats->seek_file = nullptr;
  stats->seek_file_level = -1;

  struct State {
    Saver saver;                    
    GetStats* stats;
    const ReadOptions* options;
    Slice ikey;                    // internal_key

    // ===seek compaction 机制
    // 当Match执行次数>=2次时，记录第一次执行时的 fileMetaData 和 level，
    // 用于更新计算触发compact的量，如果某个文件被记录多次，说明多次Seek到了却Get空，
    // 可能太散了，需要compact
    FileMetaData* last_file_read;  
    int last_file_read_level;

    VersionSet* vset;
    Status s;
    bool found;

    // 返回是否继续下一个文件的搜索；如果 kFound 或 kDeleted 或者 意外错误 返回 false，
    // 其他情况返回 true

    /**
     * @brief 回调函数，对 overlap saver.user_key 的 FileMetaData 执行该函数，
     * 将该 FileMetaData 所对应的table 文件导入到缓存，并对saver.user_key进行检索
     * 
     * @param arg 存储State*
     * @param level f 所在的 level
     * @param f 文件元数据
     * @return true 
     * @return false 如果 kFound 或 kDeleted 或者 意外错误 返回 false，kNotFound 返回 true
     */
    static bool Match(void* arg, int level, FileMetaData* f) {
      State* state = reinterpret_cast<State*>(arg);

      // 如果 state->stats->seek_file == nullptr (该值还没被设置过)，
      // 并且 state->last_file_read != nullptr (说明之前已经调用过Match)，
      // 综上说明如果条件成立，这是第二次调用Match的时候，
      // 记录第一次 Match 的文件metadata和Level
      if (state->stats->seek_file == nullptr &&
          state->last_file_read != nullptr) {
        // We have had more than one seek for this read.  Charge the 1st file.
        state->stats->seek_file = state->last_file_read;
        state->stats->seek_file_level = state->last_file_read_level;
      }

      state->last_file_read = f;
      state->last_file_read_level = level;

      state->s = state->vset->table_cache_->Get(*state->options, f->number,
                                                f->file_size, state->ikey,
                                                &state->saver, SaveValue);

      // 只有一些意外的错误 ok 为false，其他情况即使key不存在，也会设置ok==true
      if (!state->s.ok()) {
        state->found = true;
        return false;
      }
      switch (state->saver.state) {
        case kNotFound:
          return true;  // Keep searching in other files
        case kFound:
          state->found = true;
          return false;
        case kDeleted:
          return false;
        case kCorrupt:
          state->s =
              Status::Corruption("corrupted key for ", state->saver.user_key);
          state->found = true;
          return false;
      }

      // Not reached. Added to avoid false compilation warnings of
      // "control reaches end of non-void function".
      return false;
    }
  };

  // 定义get所需的State，并初始化
  State state;
  state.found = false;
  state.stats = stats;
  state.last_file_read = nullptr;
  state.last_file_read_level = -1;  // 这仅发生在level 0的情况下

  state.options = &options;
  state.ikey = k.internal_key();
  state.vset = vset_;

  state.saver.state = kNotFound;
  state.saver.ucmp = vset_->icmp_.user_comparator();
  state.saver.user_key = k.user_key();
  state.saver.value = value;

  ForEachOverlapping(state.saver.user_key, state.ikey, &state, &State::Match);

  return state.found ? state.s : Status::NotFound(Slice());
}

bool Version::UpdateStats(const GetStats& stats) {
  FileMetaData* f = stats.seek_file;
  if (f != nullptr) {
    f->allowed_seeks--;
    if (f->allowed_seeks <= 0 && file_to_compact_ == nullptr) {
      // 如果 f->allowed_seeks 已经递减为0或以下，且当前没有该Version没有记录需要conpact的文件，
      // 将该file 的 metadata 和 level 记录到该version中
      file_to_compact_ = f;
      file_to_compact_level_ = stats.seek_file_level;
      return true;
    }
  }
  return false;
}

bool Version::RecordReadSample(Slice internal_key) {
  ParsedInternalKey ikey;
  if (!ParseInternalKey(internal_key, &ikey)) {
    return false;
  }

  struct State {
    GetStats stats;  // Holds first matching file
    int matches;

    // 返回true的话，会选择另一个较新的进行match
    static bool Match(void* arg, int level, FileMetaData* f) {
      State* state = reinterpret_cast<State*>(arg);
      state->matches++;
      if (state->matches == 1) {
        // Remember first match.
        state->stats.seek_file = f;
        state->stats.seek_file_level = level;
      }
      // We can stop iterating once we have a second match.
      return state->matches < 2;
    }
  };

  State state;
  state.matches = 0;
  ForEachOverlapping(ikey.user_key, internal_key, &state, &State::Match);

  // Must have at least two matches since we want to merge across
  // files. But what if we have a single file that contains many
  // overwrites and deletions?  Should we have another mechanism for
  // finding such files?
  // 必须至少有两个匹配项，因为我们要跨文件合并。
  // 但是，如果我们有一个包含许多覆盖和删除的文件怎么办？
  // 我们是否应该有另一种机制来查找此类文件？
  if (state.matches >= 2) {
    // 1MB cost is about 1 seek (see comment in Builder::Apply).
    return UpdateStats(state.stats);
  }
  return false;
}

void Version::Ref() { ++refs_; }

void Version::Unref() {
  assert(this != &vset_->dummy_versions_);
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    delete this;
  }
}

/**
 * @brief 如果指定 $level 的某些文件 overlap [*smallest_user_key,*largest_user_key] ，
 * 则返回 true；否则返回true；
 * 
 * @param level 
 * @param smallest_user_key user_key下界
 * @param largest_user_key user key上界
 * @return true 存在overlap
 * @return false  不存在overlap
 */
bool Version::OverlapInLevel(int level, const Slice* smallest_user_key,
                             const Slice* largest_user_key) {
  return SomeFileOverlapsRange(vset_->icmp_, (level > 0), files_[level],
                               smallest_user_key, largest_user_key);
}

/**
 * @brief 给定memtable中key的范围 [smallest_user_key,largest_user_key]，
 * 返回其应该被落盘到哪个level
 * 
 * @param smallest_user_key   MemTable key 范围下界
 * @param largest_user_key    MemTable key 范围上界
 * @return int 应该落盘的level
 */
int Version::PickLevelForMemTableOutput(const Slice& smallest_user_key,
                                        const Slice& largest_user_key) {
  int level = 0;

  if (!OverlapInLevel(0, &smallest_user_key, &largest_user_key)) {

    // 1. 如果level 0 和 [smallest_user_key, largest_user_key] 没有overlap，
    // 可以进行进一步处理，尝试落盘到更底层
    // Push to next level if there is no overlap in next level,
    // and the #bytes overlapping in the level after that are limited.
    InternalKey start(smallest_user_key, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey limit(largest_user_key, 0, static_cast<ValueType>(0));
    std::vector<FileMetaData*> overlaps;

    // kMaxMemCompactLevel 默认为2
    while (level < config::kMaxMemCompactLevel) {

      // 1-1. 如果level+1 与 [smallest_user_key, largest_user_key] 有重复，直接break
      if (OverlapInLevel(level + 1, &smallest_user_key, &largest_user_key)) {
        break;
      }

      // 1-2. 如果 level + 2 < config::kNumLevels，判断grandparent的情况
      if (level + 2 < config::kNumLevels) {
        // 在level+2 中，找到和 [start, end] 有重合的 fileMetaData
        // Check that file does not overlap too many grandparent bytes.
        GetOverlappingInputs(level + 2, &start, &limit, &overlaps);
        const int64_t sum = TotalFileSize(overlaps);

        // 如果将 memtable 落盘到 level+1中，那么未来如果和 level+2 compact，
        // 需要读取的数据太多，所以算了，还是compact到level中
        if (sum > MaxGrandParentOverlapBytes(vset_->options_)) {
          break;
        }
      }

      // 可以落盘(minior compaction)到 level++
      level++;
    }
  }
  return level;
}

/**
 * @brief 在指定的level中，找到和 [begin, end]的 userKey（非常关键）有重合的 fileMetaData，
 * 将其 append 到 inputs 中
 * 
 * @param level  指定level
 * @param begin 范围
 * @param end   范围
 * @param inputs out, overlap 的 fileMetaData
 */
void Version::GetOverlappingInputs(int level, const InternalKey* begin,
                                   const InternalKey* end,
                                   std::vector<FileMetaData*>* inputs) {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  inputs->clear();
  /// 1. 首先根据参数初始化查找变量, internalKey ==> userKey
  Slice user_begin, user_end;
  if (begin != nullptr) {
    user_begin = begin->user_key();
  }
  if (end != nullptr) {
    user_end = end->user_key();
  }
  const Comparator* user_cmp = vset_->icmp_.user_comparator();

  /// 2. 比较区间，对于Level-0如果在当前覆盖范围[begin,end]中找到对应的 fileMetaData，
  /// 需要进一步判断是否需要扩大[begin,end]空间，如果需要则重新检索 覆盖到的 fileMetaData
  for (size_t i = 0; i < files_[level].size();) {
    FileMetaData* f = files_[level][i++];

    const Slice file_start = f->smallest.user_key();
    const Slice file_limit = f->largest.user_key();

    if (begin != nullptr && user_cmp->Compare(file_limit, user_begin) < 0) {
      // "f" is completely before specified range; skip it
    } else if (end != nullptr && user_cmp->Compare(file_start, user_end) > 0) {
      // "f" is completely after specified range; skip it
    } else {
      inputs->push_back(f);

      // 如果是 level-0， level-0 可能 overlap，所以需要扩大区间，
      // 如果扩大了区间，则需要重新搜索
      if (level == 0) {
        // Level-0 files may overlap each other.  So check if the newly
        // added file has expanded the range.  If so, restart search.
        if (begin != nullptr && user_cmp->Compare(file_start, user_begin) < 0) {
          user_begin = file_start;
          inputs->clear();
          i = 0;
        } else if (end != nullptr &&
                   user_cmp->Compare(file_limit, user_end) > 0) {
          user_end = file_limit;
          inputs->clear();
          i = 0;
        }
      }
    }
  }
}

std::string Version::DebugString() const {
  std::string r;
  for (int level = 0; level < config::kNumLevels; level++) {
    // E.g.,
    //   --- level 1 ---
    //   17:123['a' .. 'd']
    //   20:43['e' .. 'g']
    r.append("--- level ");
    AppendNumberTo(&r, level);
    r.append(" ---\n");
    const std::vector<FileMetaData*>& files = files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      r.push_back(' ');
      AppendNumberTo(&r, files[i]->number);
      r.push_back(':');
      AppendNumberTo(&r, files[i]->file_size);
      r.append("[");
      r.append(files[i]->smallest.DebugString());
      r.append(" .. ");
      r.append(files[i]->largest.DebugString());
      r.append("]\n");
    }
  }
  return r;
}




// A helper class so we can efficiently apply a whole sequence
// of edits to a particular state without creating intermediate
// Versions that contain full copies of the intermediate state. 
class VersionSet::Builder {
 private:
  // Helper to sort by v->files_[file_number].smallest
  // 比较器：按 smallest 升序排序，如果相等再按 fileNum 升序排序
  // 如果 f1->smallest < f2->smallest，返回true；
  // 如果相等，则 f1->number < f2->number，返回true；
  struct BySmallestKey {
    const InternalKeyComparator* internal_comparator;
  
    bool operator()(FileMetaData* f1, FileMetaData* f2) const {
      int r = internal_comparator->Compare(f1->smallest, f2->smallest);
      if (r != 0) {
        return (r < 0);
      } else {
        // Break ties by file number
        return (f1->number < f2->number);
      }
    }
  };

  
  typedef std::set<FileMetaData*, BySmallestKey> FileSet;

  // 记录添加和删除的文件
  struct LevelState {
    std::set<uint64_t> deleted_files; // fileNum
    FileSet* added_files;             // FileMetaData，by BySmallestKey's order
  };

  // 以下两个成员调用者传入
  VersionSet* vset_;
  Version* base_;     // 全量版本

  // 每层的文件变化状态
  LevelState levels_[config::kNumLevels];

 public:
  // Initialize a builder with the files from *base and other info from *vset
  Builder(VersionSet* vset, Version* base) : vset_(vset), base_(base) {
    base_->Ref();
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;
    for (int level = 0; level < config::kNumLevels; level++) {
      levels_[level].added_files = new FileSet(cmp);
    }
  }

  ~Builder() {
    for (int level = 0; level < config::kNumLevels; level++) {
      const FileSet* added = levels_[level].added_files;
      std::vector<FileMetaData*> to_unref;
      to_unref.reserve(added->size());
      for (FileSet::const_iterator it = added->begin(); it != added->end();
           ++it) {
        to_unref.push_back(*it);
      }
      delete added;
      for (uint32_t i = 0; i < to_unref.size(); i++) {
        FileMetaData* f = to_unref[i];
        f->refs--;
        if (f->refs <= 0) {
          delete f;
        }
      }
    }
    base_->Unref();
  }

  // Apply all of the edits in *edit to the current state.
  // 该函数将edit中的修改apply到当前Builder和versionSet状态中
  void Apply(const VersionEdit* edit) {
    // Update compaction pointers
    /// 1. 把edit记录的compaction pointer apply到 vset 当前状态上
    for (size_t i = 0; i < edit->compact_pointers_.size(); i++) {
      const int level = edit->compact_pointers_[i].first;
      vset_->compact_pointer_[level] =
          edit->compact_pointers_[i].second.Encode().ToString();
    }

    /// 2. 把edit记录的已删除文件应用到Builder当前状态
    // Delete files
    for (const auto& deleted_file_set_kvp : edit->deleted_files_) {
      const int level = deleted_file_set_kvp.first;
      const uint64_t number = deleted_file_set_kvp.second;
      levels_[level].deleted_files.insert(number);
    }

    // Add new files
    /// 3. 把edit记录的新加文件应用到当前状态，这里会初始化文件的allowed_seeks值，
    /// 以在文件被无谓seek指定次数后自动执行compaction，这里作者阐述了其设置规则。
    for (size_t i = 0; i < edit->new_files_.size(); i++) {
      const int level = edit->new_files_[i].first;
      FileMetaData* f = new FileMetaData(edit->new_files_[i].second);
      f->refs = 1;

      // We arrange to automatically compact this file after
      // a certain number of seeks.  Let's assume:
      //   (1) One seek costs 10ms
      //   (2) Writing or reading 1MB costs 10ms (100MB/s)
      //   (3) A compaction of 1MB does 25MB of IO:
      //         1MB read from this level
      //         10-12MB read from next level (boundaries may be misaligned)
      //         10-12MB written to next level
      // This implies that 25 seeks cost the same as the compaction
      // of 1MB of data.  I.e., one seek costs approximately the
      // same as the compaction of 40KB of data.  We are a little
      // conservative and allow approximately one seek for every 16KB
      // of data before triggering a compaction.
      // 我们有点保守，在触发压缩之前允许大约每 16KB 的数据进行一次搜索。
      // 1 次 1MB seek <===> 40KB compaction
      // 保守估计 1次seek <===> 16KB compaction
      // 所以设置这样的一种机制：allowed_seeks * 16KB <= Table 文件大小，
      // 即 f->allowed_seeks = static_cast<int>((f->file_size / 16384U));
      // 如果超过该设置的阈值，就将该 Table 文件 compact。
      // 理解：对于每16KB的数据，我们允许它在触发compaction之前能做一次seek。
      // 注意：这里 allowed_seek 命名有点误导；allowed_seeks--，
      // iff 该Table是倍所要找的Key命中，并且是第一个命中的（最新的），但是却找不到key；
      // 想象：一个level = 0的Table中，只有两条数据，其中k1 = 0, k2 = 10000000000;
      // 所有 (0，10000000000)的 query_key 都会命中该块，但是却找不到数据，
      // 以此机制来促进compact
      f->allowed_seeks = static_cast<int>((f->file_size / 16384U));
      if (f->allowed_seeks < 100) f->allowed_seeks = 100;

      levels_[level].deleted_files.erase(f->number);// 以防万一，某处bug
      levels_[level].added_files->insert(f);
    }
  }

  // Save the current state in *v.
  // 当前的Builder状态存储到version中返回
  void SaveTo(Version* v) {
    BySmallestKey cmp;
    cmp.internal_comparator = &vset_->icmp_;

    /// 1. For循环遍历所有的level[0, config::kNumLevels-1]，
    /// 把新加的文件levels_[level] 和 已存在的文件base_->file_[level] merge在一起，
    /// 丢弃已删除的文件，结果保存在v中。
    /// 对于level> 0，还要确保集合中的文件没有重合
    for (int level = 0; level < config::kNumLevels; level++) {
      // Merge the set of added files with the set of pre-existing files.
      // Drop any deleted files.  Store the result in *v.

      // 已存在的文件 base_ file
      const std::vector<FileMetaData*>& base_files = base_->files_[level];
      std::vector<FileMetaData*>::const_iterator base_iter = base_files.begin();
      std::vector<FileMetaData*>::const_iterator base_end = base_files.end();

      // 新添加的文件 add file
      const FileSet* added_files = levels_[level].added_files;

      // 最终merge的输出 v
      v->files_[level].reserve(base_files.size() + added_files->size());

      // 1-1. 开始 merge 添加文件
      for (const auto& added_file : *added_files) {
        // Add all smaller files listed in base_
        for (std::vector<FileMetaData*>::const_iterator bpos =
                 std::upper_bound(base_iter, base_end, added_file, cmp);
             base_iter != bpos; ++base_iter) {
          // 把 added_file 之前的（key range）已存在文件（base_）添加到 v中
          MaybeAddFile(v, level, *base_iter);
        }
        // 把 added_file 添加到 v 中
        MaybeAddFile(v, level, added_file);
      }

      // Add remaining base files
      // 添加剩下的 base file
      for (; base_iter != base_end; ++base_iter) {
        MaybeAddFile(v, level, *base_iter);
      }

#ifndef NDEBUG
      // check：如果 level > 0 ，检查 no overlap
      // Make sure there is no overlap in levels > 0
      if (level > 0) {
        for (uint32_t i = 1; i < v->files_[level].size(); i++) {
          const InternalKey& prev_end = v->files_[level][i - 1]->largest;
          const InternalKey& this_begin = v->files_[level][i]->smallest;
          if (vset_->icmp_.Compare(prev_end, this_begin) >= 0) {
            std::fprintf(stderr, "overlapping ranges in same level %s vs. %s\n",
                         prev_end.DebugString().c_str(),
                         this_begin.DebugString().c_str());
            std::abort();
          }
        }
      }
#endif
    }
  }

  // 尝试将f加入到files_[level]文件set中;
  // 条件1： 文件不能被删除，也就是不能在levels_[level].deleted_files集合中；
  // 条件2：保证文件之间的key是连续的，即基于比较器vset_->icmp_，
  // f的min key要大于levels_[level]集合中最后一个文件的max key
  void MaybeAddFile(Version* v, int level, FileMetaData* f) {
    if (levels_[level].deleted_files.count(f->number) > 0) {
      // File is deleted: do nothing
    } else {
      std::vector<FileMetaData*>* files = &v->files_[level];
      if (level > 0 && !files->empty()) {
        // Must not overlap
        assert(vset_->icmp_.Compare((*files)[files->size() - 1]->largest,
                                    f->smallest) < 0);
      }
      f->refs++;
      files->push_back(f);
    }
  }
};

VersionSet::VersionSet(const std::string& dbname, const Options* options,
                       TableCache* table_cache,
                       const InternalKeyComparator* cmp)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      table_cache_(table_cache),
      icmp_(*cmp),
      next_file_number_(2),    // next_file_number_从2开始
      manifest_file_number_(0),  // Filled by Recover()
      last_sequence_(0),
      log_number_(0),
      prev_log_number_(0),
      descriptor_file_(nullptr),
      descriptor_log_(nullptr),
      dummy_versions_(this),
      current_(nullptr) {
  // 创建新的Version并加入到Version链表中，并设置CURRENT=新创建version
  AppendVersion(new Version(this));
}

VersionSet::~VersionSet() {
  current_->Unref();
  assert(dummy_versions_.next_ == &dummy_versions_);  // List must be empty
  delete descriptor_log_;
  delete descriptor_file_;
}


void VersionSet::AppendVersion(Version* v) {
  // Make "v" current
  assert(v->refs_ == 0);
  assert(v != current_);
  if (current_ != nullptr) {
    current_->Unref();
  }
  current_ = v;
  v->Ref();

  // Append to linked list
  v->prev_ = dummy_versions_.prev_;
  v->next_ = &dummy_versions_;
  v->prev_->next_ = v;
  v->next_->prev_ = v;
}

/**
 * @brief 将指定的VersionEdit apply 到current version上 ；
 * 如果当前没有MANIFEST文件，则创建MANIFEST文件并写入全量版本信息 curentVersion(首次打开DB时)；
 * 如果当前存在NANIFEST文件，则将 增量版本信息 edit 追加到文件中
 * 
 * @param edit 
 * @param mu 
 * @return Status 
 */
Status VersionSet::LogAndApply(VersionEdit* edit, port::Mutex* mu) {
  /// 1. 为edit设置log number等4个计数器
  if (edit->has_log_number_) {
    assert(edit->log_number_ >= log_number_);
    assert(edit->log_number_ < next_file_number_);
  } else {
    edit->SetLogNumber(log_number_);
  }

  if (!edit->has_prev_log_number_) {
    edit->SetPrevLogNumber(prev_log_number_);
  }
  edit->SetNextFile(next_file_number_);
  edit->SetLastSequence(last_sequence_);


  /// 2. 创建一个新的Version v，v_new = current_ + edit
  Version* v = new Version(this);
  {
    Builder builder(this, current_);
    builder.Apply(edit);
    builder.SaveTo(v);
  }
  Finalize(v); // 为v计算执行compaction的最佳level  

  // Initialize new descriptor log file if necessary by creating
  // a temporary file that contains a snapshot of the current version.
  /// 3. 如果MANIFEST文件指针不存在，就创建并初始化一个新的MANIFEST文件。
  /// 这只会发生在第一次打开数据库时，创建后使MANIFEST文件保存全量版本信息
  std::string new_manifest_file;
  Status s;
  if (descriptor_log_ == nullptr) {
    // No reason to unlock *mu here since we only hit this path in the
    // first call to LogAndApply (when opening the database).
    assert(descriptor_file_ == nullptr);
    new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_);
    s = env_->NewWritableFile(new_manifest_file, &descriptor_file_);
    if (s.ok()) {
      descriptor_log_ = new log::Writer(descriptor_file_);
      s = WriteSnapshot(descriptor_log_);
    }
  }

  /// 4. 向MANIFEST写入一条新的log，记录 edit 中的增量信息，即写入edit。
  /// 在文件写操作时unlock锁，写入完成后，再重新lock，以防止浪费在长时间的IO操作上
  // Unlock during expensive MANIFEST log write
  {
    mu->Unlock();

    // Write new record to MANIFEST log
    if (s.ok()) {
      std::string record;
      edit->EncodeTo(&record);
      s = descriptor_log_->AddRecord(record);
      if (s.ok()) {
        s = descriptor_file_->Sync();
      }
      if (!s.ok()) {
        Log(options_->info_log, "MANIFEST write: %s\n", s.ToString().c_str());
      }
    }

    // If we just created a new descriptor file, install it by writing a
    // new CURRENT file that points to it.
    // 如果刚才创建了一个MANIFEST文件，通过写一个指向它的CURRENT文件  
    // 安装它；不需要再次检查MANIFEST是否出错，因为如果出错后面会删除它 
    if (s.ok() && !new_manifest_file.empty()) {
      s = SetCurrentFile(env_, dbname_, manifest_file_number_);
    }

    mu->Lock();
  }

  // Install the new version
  /// 5. 安装这个新的version
  if (s.ok()) {
    AppendVersion(v);
    log_number_ = edit->log_number_;
    prev_log_number_ = edit->prev_log_number_;
  } else {
    delete v;
    if (!new_manifest_file.empty()) {
      delete descriptor_log_;
      delete descriptor_file_;
      descriptor_log_ = nullptr;
      descriptor_file_ = nullptr;

      env_->RemoveFile(new_manifest_file);
    }
  }

  return s;
}


/**
 * @brief 从磁盘的MANIFEST恢复最后持久化的元数据信息，将其 apply 到 一个新生成的Version中，
 * 并更新 VersionSet 的状态
 * 
 * @param save_manifest 是否需要重新生成一个新的MANIFEST文件 (当current MANIFEST文件太大时需要)
 * @return Status 
 */
Status VersionSet::Recover(bool* save_manifest) {
  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    void Corruption(size_t bytes, const Status& s) override {
      if (this->status->ok()) *this->status = s;
    }
  };

  /// 1. 读取CURRENT文件，获得最新的MANIFEST文件名，根据文件名打开MANIFEST文件。
  // Read "CURRENT" file, which contains a pointer to the current manifest file
  std::string current;
  Status s = ReadFileToString(env_, CurrentFileName(dbname_), &current);
  if (!s.ok()) {
    return s;
  }
  // CURRENT文件以\n结尾，读取后需要trim下
  if (current.empty() || current[current.size() - 1] != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  current.resize(current.size() - 1);


  std::string dscname = dbname_ + "/" + current;
  SequentialFile* file; // 顺序读的文件对象
  s = env_->NewSequentialFile(dscname, &file);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      return Status::Corruption("CURRENT points to a non-existent file",
                                s.ToString());
    }
    return s;
  }

  bool have_log_number = false;
  bool have_prev_log_number = false;
  bool have_next_file = false;
  bool have_last_sequence = false;
  uint64_t next_file = 0;
  uint64_t last_sequence = 0;
  uint64_t log_number = 0;
  uint64_t prev_log_number = 0;
  Builder builder(this, current_);
  int read_records = 0;   // 记录读取的 record 数

  /// 2. 读取MANIFEST内容，MANIFEST是以log的方式写入的，因此这里调用的是log::Reader来读取
  {
    // Log Reader 相关
    LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(file, &reporter, true /*checksum*/,
                       0 /*initial_offset*/);

    // 承载获取的 log record
    Slice record;
    std::string scratch;

    while (reader.ReadRecord(&record, &scratch) && s.ok()) {
      ++read_records;
      VersionEdit edit;
      // 2-1 调用VersionEdit::DecodeFrom，从内容解析出VersionEdit对象
      s = edit.DecodeFrom(record);

      // 2-2 check compactor Name
      if (s.ok()) {
        if (edit.has_comparator_ &&
            edit.comparator_ != icmp_.user_comparator()->Name()) {
          s = Status::InvalidArgument(
              edit.comparator_ + " does not match existing comparator ",
              icmp_.user_comparator()->Name());
        }
      }

      /// 2-3 将VersionEdit apply到versionset和Builder中
      if (s.ok()) {
        builder.Apply(&edit);
      }


      /// 2-3 读取MANIFEST中的log number, prev log number, nextfile number, last sequence
      if (edit.has_log_number_) {
        log_number = edit.log_number_;
        have_log_number = true;
      }

      if (edit.has_prev_log_number_) {
        prev_log_number = edit.prev_log_number_;
        have_prev_log_number = true;
      }

      if (edit.has_next_file_number_) {
        next_file = edit.next_file_number_;
        have_next_file = true;
      }

      if (edit.has_last_sequence_) {
        last_sequence = edit.last_sequence_;
        have_last_sequence = true;
      }

      // loop
    }
  }
  delete file;
  file = nullptr;



  if (s.ok()) {
    /// 3. check 下是否读取了nextFile Num、logFile Num、lastSeq
    // （说明这几个在Manifest文件中必须存在），
    // 将读取到的log number, prev log number标记为已使用
    if (!have_next_file) {
      s = Status::Corruption("no meta-nextfile entry in descriptor");
    } else if (!have_log_number) {
      s = Status::Corruption("no meta-lognumber entry in descriptor");
    } else if (!have_last_sequence) {
      s = Status::Corruption("no last-sequence-number entry in descriptor");
    }

    if (!have_prev_log_number) {
      prev_log_number = 0;
    }
    MarkFileNumberUsed(prev_log_number);
    MarkFileNumberUsed(log_number);
  }


  
  if (s.ok()) {
    /// 4. 创建新的Version v，将builder apply 到 v 中，并将 v append 到双向循环链表，
    /// 同时调用Finalize计算size compact，并用前面读取的几个 num 初始化Version的内部数据结构
    Version* v = new Version(this);

    builder.SaveTo(v);
    // Install recovered version
    // 4-1. Finalize(v)和AppendVersion(v)用来安装并使用version v，
    // 在AppendVersion函数中会将current version设置为v
    Finalize(v);
    AppendVersion(v);
    manifest_file_number_ = next_file;
    next_file_number_ = next_file + 1;
    last_sequence_ = last_sequence;
    log_number_ = log_number;
    prev_log_number_ = prev_log_number;

    /// 5. 判断是否Manifest 文件可重用，如果可重用，
    /// 设置 descriptor_log_ 和 manifest_file_number_
    // See if we can reuse the existing MANIFEST file.
    if (ReuseManifest(dscname, current)) {
      // No need to save new manifest
    } else {
      *save_manifest = true;
    }
  } else {
    std::string error = s.ToString();
    Log(options_->info_log, "Error recovering version set with %d records: %s",
        read_records, error.c_str());
  }

  return s;
}


/**
 * @brief 返回是否继续使用当前的 Manifest 文件，可能存在 Manifest 文件过大，需要更换的情况;
 * 如果可以重用，内部设置 descriptor_log_ 和 manifest_file_number_
 * 
 * @param dscname fileNum
 * @param dscbase DBName(DBpath too)
 * @return true 表示可重用，false表示不可重用
 */
bool VersionSet::ReuseManifest(const std::string& dscname,
                               const std::string& dscbase) {
  if (!options_->reuse_logs) {
    return false;
  }
  FileType manifest_type;
  uint64_t manifest_number;
  uint64_t manifest_size;
  // 1. 如果超过大小，返回false，表示不重用Manifest
  if (!ParseFileName(dscbase, &manifest_number, &manifest_type) ||
      manifest_type != kDescriptorFile ||
      !env_->GetFileSize(dscname, &manifest_size).ok() ||
      // Make new compacted MANIFEST if old one is too big
      manifest_size >= TargetFileSize(options_)) {
    return false;
  }


  // 2. 重用Manifest，并设置VersionSet的状态
  assert(descriptor_file_ == nullptr);
  assert(descriptor_log_ == nullptr);
  Status r = env_->NewAppendableFile(dscname, &descriptor_file_);
  if (!r.ok()) {
    Log(options_->info_log, "Reuse MANIFEST: %s\n", r.ToString().c_str());
    assert(descriptor_file_ == nullptr);
    return false;
  }

  Log(options_->info_log, "Reusing MANIFEST %s\n", dscname.c_str());
  descriptor_log_ = new log::Writer(descriptor_file_, manifest_size);
  manifest_file_number_ = manifest_number;
  return true;
}

/**
 * @brief 标记指定的文件编号已经被使用了，内部更新 next_file_number_
 * 
 * @param number 
 */
void VersionSet::MarkFileNumberUsed(uint64_t number) {
  if (next_file_number_ <= number) {
    next_file_number_ = number + 1;
  }
}

/**
 * @brief 对于当前版本 v，计算最佳的压缩 level，并计算一个 compaction 分数，
 * 最终设置 $v 的 compaction_score_ 和 compaction_level_
 * 
 * @param v 
 */
void VersionSet::Finalize(Version* v) {
  // Precomputed best level for next compaction
  int best_level = -1;
  double best_score = -1;

  for (int level = 0; level < config::kNumLevels - 1; level++) {
    double score;
    // 1. 对于level 0以文件个数计算，kL0_CompactionTrigger默认配置为4
    if (level == 0) {
      // We treat level-0 specially by bounding the number of files
      // instead of number of bytes for two reasons:
      //
      // (1) With larger write-buffer sizes, it is nice not to do too
      // many level-0 compactions.
      // 对于较大的写入缓冲区大小，最好不要进行太多的 0 级压缩。
      //
      // (2) The files in level-0 are merged on every read and
      // therefore we wish to avoid too many files when the individual
      // file size is small (perhaps because of a small write-buffer
      // setting, or very high compression ratios, or lots of
      // overwrites/deletions).
      // level-0 中的文件在每次读取时都会合并，因此我们希望在单个文件较小时
      // （可能是因为写入缓冲区设置较小，或压缩率非常高，或大量覆盖/删除 ）避免文件过多
      score = v->files_[level].size() /
              static_cast<double>(config::kL0_CompactionTrigger);
    } else {
      /// 2. 对于 level >= 1，根据level内的文件总大小计算
      // Compute the ratio of current size to size limit.
      const uint64_t level_bytes = TotalFileSize(v->files_[level]);
      score =
          static_cast<double>(level_bytes) / MaxBytesForLevel(options_, level);
    }

    if (score > best_score) {
      best_level = level;
      best_score = score;
    }
  }
  // 3. 最后把计算结果保存到v的两个成员compaction_level_和compaction_score_中
  v->compaction_level_ = best_level;
  v->compaction_score_ = best_score;
}

Status VersionSet::WriteSnapshot(log::Writer* log) {
  // TODO: Break up into multiple records to reduce memory usage on recovery?

  // Save metadata
  /// 1. edit 记录 compactor name
  VersionEdit edit;
  edit.SetComparatorName(icmp_.user_comparator()->Name());

  // Save compaction pointers
  /// 2. edit 记录  compaction pointers
  for (int level = 0; level < config::kNumLevels; level++) {
    if (!compact_pointer_[level].empty()) {
      InternalKey key;
      key.DecodeFrom(compact_pointer_[level]);
      edit.SetCompactPointer(level, key);
    }
  }

  // Save files
  /// 3. edit 记录各个层次的文件
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = current_->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      const FileMetaData* f = files[i];
      edit.AddFile(level, f->number, f->file_size, f->smallest, f->largest);
    }
  }
  
  /// 4. edit encode to string
  std::string record;
  edit.EncodeTo(&record);

  /// 5. 持久化 meta record
  return log->AddRecord(record);
}

int VersionSet::NumLevelFiles(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return current_->files_[level].size();
}

const char* VersionSet::LevelSummary(LevelSummaryStorage* scratch) const {
  // Update code if kNumLevels changes
  static_assert(config::kNumLevels == 7, "");
  std::snprintf(
      scratch->buffer, sizeof(scratch->buffer), "files[ %d %d %d %d %d %d %d ]",
      int(current_->files_[0].size()), int(current_->files_[1].size()),
      int(current_->files_[2].size()), int(current_->files_[3].size()),
      int(current_->files_[4].size()), int(current_->files_[5].size()),
      int(current_->files_[6].size()));
  return scratch->buffer;
}

/**
 * @brief 在特定的某个version v中，返回key对应的一个大概的offset
 * 
 * @param v 指定Version
 * @param ikey 指定 InternalKey
 * @return uint64_t 
 */
uint64_t VersionSet::ApproximateOffsetOf(Version* v, const InternalKey& ikey) {
  uint64_t result = 0;
  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMetaData*>& files = v->files_[level];
    for (size_t i = 0; i < files.size(); i++) {
      if (icmp_.Compare(files[i]->largest, ikey) <= 0) {
        // Entire file is before "ikey", so just add the file size
        result += files[i]->file_size;
      } else if (icmp_.Compare(files[i]->smallest, ikey) > 0) {
        // Entire file is after "ikey", so ignore
        if (level > 0) {
          // Files other than level 0 are sorted by meta->smallest, so
          // no further files in this level will contain data for
          // "ikey".
          break;
        }
      } else {
        // small_key |   ikey | largest_key 的情况
        // "ikey" falls in the range for this table.  Add the
        // approximate offset of "ikey" within the table.
        Table* tableptr;
        Iterator* iter = table_cache_->NewIterator(
            ReadOptions(), files[i]->number, files[i]->file_size, &tableptr);
        if (tableptr != nullptr) {
          result += tableptr->ApproximateOffsetOf(ikey.Encode());
        }
        delete iter;
      }
    }
  }
  return result;
}

void VersionSet::AddLiveFiles(std::set<uint64_t>* live) {
  for (Version* v = dummy_versions_.next_; v != &dummy_versions_;
       v = v->next_) {
    for (int level = 0; level < config::kNumLevels; level++) {
      const std::vector<FileMetaData*>& files = v->files_[level];
      for (size_t i = 0; i < files.size(); i++) {
        live->insert(files[i]->number);
      }
    }
  }
}

int64_t VersionSet::NumLevelBytes(int level) const {
  assert(level >= 0);
  assert(level < config::kNumLevels);
  return TotalFileSize(current_->files_[level]);
}

/**
 * @brief 对于所有level>=1的情况，找到level>=1中，
 * 一个文件（level）和下一层文件的重叠数据的文件大小最大值
 * 
 * @return int64_t 
 */
int64_t VersionSet::MaxNextLevelOverlappingBytes() {
  int64_t result = 0;
  std::vector<FileMetaData*> overlaps;
  for (int level = 1; level < config::kNumLevels - 1; level++) {
    for (size_t i = 0; i < current_->files_[level].size(); i++) {
      const FileMetaData* f = current_->files_[level][i];
      current_->GetOverlappingInputs(level + 1, &f->smallest, &f->largest,
                                     &overlaps);
      const int64_t sum = TotalFileSize(overlaps);
      if (sum > result) {
        result = sum;
      }
    }
  }
  return result;
}

// Stores the minimal range that covers all entries in inputs in
// *smallest, *largest.
// REQUIRES: inputs is not empty
// 返回 包含inputs中所有key range的最小范围，[smallest,largest]为该范围的输出参数
void VersionSet::GetRange(const std::vector<FileMetaData*>& inputs,
                          InternalKey* smallest, InternalKey* largest) {
  assert(!inputs.empty());
  smallest->Clear();
  largest->Clear();
  for (size_t i = 0; i < inputs.size(); i++) {
    FileMetaData* f = inputs[i];
    if (i == 0) {
      *smallest = f->smallest;
      *largest = f->largest;
    } else {
      if (icmp_.Compare(f->smallest, *smallest) < 0) {
        *smallest = f->smallest;
      }
      if (icmp_.Compare(f->largest, *largest) > 0) {
        *largest = f->largest;
      }
    }
  }
}

// Stores the minimal range that covers all entries in inputs1 and inputs2
// in *smallest, *largest.
// REQUIRES: inputs is not empty
/**
 * @brief 获取覆盖 inputs1 和 inputs2 中的最小range
 * 
 * @param inputs1  files
 * @param inputs2 files
 * @param smallest out,范围
 * @param largest out，范围 
 */
void VersionSet::GetRange2(const std::vector<FileMetaData*>& inputs1,
                           const std::vector<FileMetaData*>& inputs2,
                           InternalKey* smallest, InternalKey* largest) {
  std::vector<FileMetaData*> all = inputs1;
  all.insert(all.end(), inputs2.begin(), inputs2.end());
  GetRange(all, smallest, largest);
}

/**
 * @brief 返回一个遍历 c 中数据的迭代器， MergingIterator
 * 
 * @param c 
 * @return Iterator* 
 */
Iterator* VersionSet::MakeInputIterator(Compaction* c) {
  ReadOptions options;
  options.verify_checksums = options_->paranoid_checks;
  options.fill_cache = false;

  // Level-0 files have to be merged together.  For other levels,
  // we will make a concatenating iterator per level.
  // TODO(opt): use concatenating iterator for level-0 if there is no overlap
  // (1). c->inputs_[0].size() 表示，几个level 0 文件，就多少个 iterator，+1 表示level+1的文件遍历
  // (2). 2,一个存level的迭代器，一个存level+1的
  const int space = (c->level() == 0 ? c->inputs_[0].size() + 1 : 2);
  Iterator** list = new Iterator*[space];
  int num = 0;
  /// 1. 分level == 0 和 level >= 1 的情况，获取compact的那两层的 iterator
  for (int which = 0; which < 2; which++) {
    if (!c->inputs_[which].empty()) {
      if (c->level() + which == 0) {
        // 如果是level 0
        const std::vector<FileMetaData*>& files = c->inputs_[which];
        for (size_t i = 0; i < files.size(); i++) {
          list[num++] = table_cache_->NewIterator(options, files[i]->number,
                                                  files[i]->file_size);
        }
      } else {
        // 其他level
        // Create concatenating iterator for the files from this level
        list[num++] = NewTwoLevelIterator(
            new Version::LevelFileNumIterator(icmp_, &c->inputs_[which]),
            &GetFileIterator, table_cache_, options);
      }
    }
  }

  // 将上面的iterator归并，构造成一个 MergingIterator
  assert(num <= space);
  Iterator* result = NewMergingIterator(&icmp_, list, num);
  delete[] list;
  return result;
}

/**
 * @brief 若在size compaction 和 seek compaction 机制中，有满足可compact的，
 * 将其compaction封装成Compaction对象返回。优先级上：size compaction > seek compaction
 * 
 * @return Compaction* 
 */
Compaction* VersionSet::PickCompaction() {
  Compaction* c;
  int level;

  // We prefer compactions triggered by too much data in a level over
  // the compactions triggered by seeks.
  // 优先级上：size compaction > seek compaction
  const bool size_compaction = (current_->compaction_score_ >= 1);
  const bool seek_compaction = (current_->file_to_compact_ != nullptr);
  if (size_compaction) {
    /// 1. 如果有 size compaction，则构造其 compaction 对象
    level = current_->compaction_level_;
    assert(level >= 0);
    assert(level + 1 < config::kNumLevels);
    c = new Compaction(options_, level);

    /// 1-1. 遍历 compaction_level_ 的FileMetaData，如果找到其上界大于compact_point的，
    /// 将其push到 c->inputs_[0],然后break，如果没有则将该level第一个 fileMetaData push
    // Pick the first file that comes after compact_pointer_[level]
    for (size_t i = 0; i < current_->files_[level].size(); i++) {
      FileMetaData* f = current_->files_[level][i];
      if (compact_pointer_[level].empty() ||
          icmp_.Compare(f->largest.Encode(), compact_pointer_[level]) > 0) {
        c->inputs_[0].push_back(f);
        break;
      }
    }
    if (c->inputs_[0].empty()) {
      // Wrap-around to the beginning of the key space
      c->inputs_[0].push_back(current_->files_[level][0]);
    }
  } else if (seek_compaction) {
    /// 2. 如果有 seek_compaction 构造其 compaction 对象
    level = current_->file_to_compact_level_;
    c = new Compaction(options_, level);
    c->inputs_[0].push_back(current_->file_to_compact_);
  } else {
    return nullptr;
  }

  
  c->input_version_ = current_;
  c->input_version_->Ref();

  // Files in level 0 may overlap each other, so pick up all overlapping ones
  // 3. 对与 level == 0，应该选择与待compact文件overlap的所有文件，一起进行compact
  if (level == 0) {
    InternalKey smallest, largest;
    GetRange(c->inputs_[0], &smallest, &largest);
    // Note that the next call will discard the file we placed in
    // c->inputs_[0] earlier and replace it with an overlapping set
    // which will include the picked file.
    current_->GetOverlappingInputs(0, &smallest, &largest, &c->inputs_[0]);
    assert(!c->inputs_[0].empty());
  }

  /// 4. 设置 Compaction 的其他输入信息（当前只设置 c->inputs_[0] 的文件信息）
  SetupOtherInputs(c);

  return c;
}


/**
 * @brief  Finds the largest key in a vector of files. Returns true if files is not empty.
 * 只要 files 不为空，就返回true， largest 记录 files 当中最大的 InternalKey(遍历查找)
 * @param icmp  比较器
 * @param files  FileMetaDatas
 * @param largest_key out, 最大的key
 * @return true files not empty
 */
bool FindLargestKey(const InternalKeyComparator& icmp,
                    const std::vector<FileMetaData*>& files,
                    InternalKey* largest_key) {
  if (files.empty()) {
    return false;
  }
  *largest_key = files[0]->largest;
  for (size_t i = 1; i < files.size(); ++i) {
    FileMetaData* f = files[i];
    if (icmp.Compare(f->largest, *largest_key) > 0) {
      *largest_key = f->largest;
    }
  }
  return true;
}

// Finds minimum file b2=(l2, u2) in level file for which l2 > u1 and
// user_key(l2) = user_key(u1)
/**
 * @brief Finds minimum file b2=(l2, u2) in level file for which l2 > u1 and 
 * user_key(l2) = user_key(u1)，在指定层的FileMetaDatas中，查找 user_key(l2) = user_key(u1)
 * 的FileMetaData
 * 
 * @param icmp  比较器
 * @param level_files 某一层的文件集合(to find b2)
 * @param largest_key 最大InternalKey(b1's largest_key)
 * @return FileMetaData* 返回符合条件的FileMetaData，没找到返回nullptr
 */
FileMetaData* FindSmallestBoundaryFile(
    const InternalKeyComparator& icmp,
    const std::vector<FileMetaData*>& level_files,
    const InternalKey& largest_key) {
  const Comparator* user_cmp = icmp.user_comparator();
  FileMetaData* smallest_boundary_file = nullptr;
  for (size_t i = 0; i < level_files.size(); ++i) {
    FileMetaData* f = level_files[i];
    // a. f->smallest > largest_key &&  f->smallest.user_key() == largest_key.user_key()
    if (icmp.Compare(f->smallest, largest_key) > 0 &&
        user_cmp->Compare(f->smallest.user_key(), largest_key.user_key()) ==
            0) {
       // b. smallest_boundary_file is the the smallest one           
      if (smallest_boundary_file == nullptr ||
          icmp.Compare(f->smallest, smallest_boundary_file->smallest) < 0) {
        smallest_boundary_file = f;
      }
    }
  }
  return smallest_boundary_file;
}

// Extracts the largest file b1 from |compaction_files| and then searches for a
// b2 in |level_files| for which user_key(u1) = user_key(l2). If it finds such a
// file b2 (known as a boundary file) it adds it to |compaction_files| and then
// searches again using this new upper bound.
//
// If there are two blocks, b1=(l1, u1) and b2=(l2, u2) and
// user_key(u1) = user_key(l2), and if we compact b1 but not b2 then a
// subsequent get operation will yield an incorrect result because it will
// return the record from b2 in level i rather than from b1 because it searches
// level by level for records matching the supplied user key.
//
// parameters:
//   in     level_files:      List of files to search for boundary files.
//   in/out compaction_files: List of files to extend by adding boundary files.
/**
 * @brief 在 level_files 中，如果有下界的userKey与compaction_files中 largest_key
 * 的UserKey overlap的file，则将其加入到compaction_files，重复此过程，则到不存在为止
 * @param icmp  比较器
 * @param level_files 某一层的 FileMetaDatas
 * @param compaction_files out,待compact的 FileMetaDatas
 */
void AddBoundaryInputs(const InternalKeyComparator& icmp,
                       const std::vector<FileMetaData*>& level_files,
                       std::vector<FileMetaData*>* compaction_files) {
  InternalKey largest_key;

  // Quick return if compaction_files is empty.
  /// 1. 找到 compaction_files 中最大的 internalKey
  if (!FindLargestKey(icmp, *compaction_files, &largest_key)) {
    return;
  }

  bool continue_searching = true;

  // 迭代查找
  /// 2. 迭代查找有没有files（假设其key range为[l1,u1]）
  /// l1.userKey == largest_key.userKey的情况（证明另一个文件中存在相同userKey，但是更旧），
  /// 需要将其加入到compaction过程
  while (continue_searching) {
    FileMetaData* smallest_boundary_file =
        FindSmallestBoundaryFile(icmp, level_files, largest_key);

    // If a boundary file was found advance largest_key, otherwise we're done.
    if (smallest_boundary_file != NULL) {
      compaction_files->push_back(smallest_boundary_file);
      largest_key = smallest_boundary_file->largest;
    } else {
      continue_searching = false;
    }
  }
}

/**
 * @brief 设置 Compaction 的其他输入信息（当前只设置 c->inputs_[0] 的文件信息）
 * 
 * @param c 待 Compact 的 Compaction 对象
 */
void VersionSet::SetupOtherInputs(Compaction* c) {
  const int level = c->level();
  InternalKey smallest, largest;

  /// 1. expand c->inputs_[0]。如果c->inputs_[0]中的 largestKey,
  /// 与current_->files_[level]中某个file的smallestKey，存在
  /// smallestKey >  largestKey && largestKey.UserKey == smallestKey.UserKey,
  /// 更新largestKey，迭代此过程
  AddBoundaryInputs(icmp_, current_->files_[level], &c->inputs_[0]);

  /// 2. 获取 c->inputs_[0] 指定的 files的 key range
  GetRange(c->inputs_[0], &smallest, &largest);

  /// 3. 查找level+1层，overlap的文件的fileMetaDatas, output to &c->inputs_[1]
  current_->GetOverlappingInputs(level + 1, &smallest, &largest,
                                 &c->inputs_[1]);
  AddBoundaryInputs(icmp_, current_->files_[level + 1], &c->inputs_[1]);

  // Get entire range covered by compaction
  /// 4. 获取整个 compact 中 涉及的最小range，[all_start,all_limit]
  InternalKey all_start, all_limit;
  GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);

  // See if we can grow the number of inputs in "level" without
  // changing the number of "level+1" files we pick up.
  // 5. 以[all_start,all_limit]为界，尝试扩大 c->inputs_[0]
  if (!c->inputs_[1].empty()) {
    // 5-1 expend  c->inputs_[0]
    std::vector<FileMetaData*> expanded0;
    current_->GetOverlappingInputs(level, &all_start, &all_limit, &expanded0);
    AddBoundaryInputs(icmp_, current_->files_[level], &expanded0);
    const int64_t inputs0_size = TotalFileSize(c->inputs_[0]);
    const int64_t inputs1_size = TotalFileSize(c->inputs_[1]);
    const int64_t expanded0_size = TotalFileSize(expanded0);

    if (expanded0.size() > c->inputs_[0].size() &&
        inputs1_size + expanded0_size <
            ExpandedCompactionByteSizeLimit(options_)) {
      // 5-2 如果compact的文件大小没超过限制，expend  c->inputs_[1]
      InternalKey new_start, new_limit;
      GetRange(expanded0, &new_start, &new_limit);
      std::vector<FileMetaData*> expanded1;
      current_->GetOverlappingInputs(level + 1, &new_start, &new_limit,
                                     &expanded1);
      AddBoundaryInputs(icmp_, current_->files_[level + 1], &expanded1);

      // 5-3 如果 c->inputs_[1] 大小依然没变，则允许 expend c->inputs_[0]
      if (expanded1.size() == c->inputs_[1].size()) {
        Log(options_->info_log,
            "Expanding@%d %d+%d (%ld+%ld bytes) to %d+%d (%ld+%ld bytes)\n",
            level, int(c->inputs_[0].size()), int(c->inputs_[1].size()),
            long(inputs0_size), long(inputs1_size), int(expanded0.size()),
            int(expanded1.size()), long(expanded0_size), long(inputs1_size));
        smallest = new_start;
        largest = new_limit;
        c->inputs_[0] = expanded0;
        c->inputs_[1] = expanded1;
        GetRange2(c->inputs_[0], c->inputs_[1], &all_start, &all_limit);
      }
    }
  }

  // Compute the set of grandparent files that overlap this compaction
  // (parent == level+1; grandparent == level+2)
  /// 6. 设置 c->grandparents_
  if (level + 2 < config::kNumLevels) {
    current_->GetOverlappingInputs(level + 2, &all_start, &all_limit,
                                   &c->grandparents_);
  }

  // Update the place where we will do the next compaction for this level.
  // We update this immediately instead of waiting for the VersionEdit
  // to be applied so that if the compaction fails, we will try a different
  // key range next time.
  // 7. 更新compaction point，并将其 apply 到 版本信息增量edit中
  compact_pointer_[level] = largest.Encode().ToString();
  c->edit_.SetCompactPointer(level, largest);
}

/**
 * @brief 返回针对指定层数的某个范围的文件进行compact的 Compaction对象，
 * 如果 begin，end 在当前层数没有overlap到，则返回nullptr，调用者应该负责delete该返回结果
 * 
 * @param level 指定层数
 * @param begin 范围
 * @param end   范围
 * @return Compaction* 
 */
Compaction* VersionSet::CompactRange(int level, const InternalKey* begin,
                                     const InternalKey* end) {
  /// 1. 获取该范围映射到的 fileMetaDatas，将其存到 inputs 中
  std::vector<FileMetaData*> inputs;
  current_->GetOverlappingInputs(level, begin, end, &inputs);
  if (inputs.empty()) {
    return nullptr;
  }

  // Avoid compacting too much in one shot in case the range is large.
  // But we cannot do this for level-0 since level-0 files can overlap
  // and we must not pick one file and drop another older file if the
  // two files overlap.
  // 如果范围很大，请避免一次压缩太多。
  // 但是我们不能对 level-0 执行此操作，因为 level-0 文件可以overlap，
  // 如果两个文件overlap，我们不能选择一个文件并删除另一个较旧的文件。

  /// 2. 如果 inputs 里文件总大小超过 MaxFileSizeForLevel()默认2MB，
  /// 则对待compact的文件进行截断
  if (level > 0) {
    const uint64_t limit = MaxFileSizeForLevel(options_, level);
    uint64_t total = 0;
    for (size_t i = 0; i < inputs.size(); i++) {
      uint64_t s = inputs[i]->file_size;
      total += s;
      if (total >= limit) {
        inputs.resize(i + 1); // 
        break;
      }
    }
  }

  /// 3. 构造Compaction，写入c->inputs_[0]
  Compaction* c = new Compaction(options_, level);
  c->input_version_ = current_;
  c->input_version_->Ref();
  c->inputs_[0] = inputs;

  /// 4. 写入compaction的其他数据
  SetupOtherInputs(c);
  return c;
}

Compaction::Compaction(const Options* options, int level)
    : level_(level),
      max_output_file_size_(MaxFileSizeForLevel(options, level)),
      input_version_(nullptr),
      grandparent_index_(0),
      seen_key_(false),
      overlapped_bytes_(0) {
  for (int i = 0; i < config::kNumLevels; i++) {
    level_ptrs_[i] = 0;
  }
}

Compaction::~Compaction() {
  if (input_version_ != nullptr) {
    input_version_->Unref();
  }
}

/**
 * @brief 返回是不是 trivial compaction；trivial compaction 的含义为，
 * 待compact的level层文件与level+1没有overlap，可以直接落到 level+1 中
 * 
 * @return true 表示是 trivial compaction
 * @return false 不是
 */
bool Compaction::IsTrivialMove() const {
  const VersionSet* vset = input_version_->vset_;
  // Avoid a move if there is lots of overlapping grandparent data.
  // Otherwise, the move could create a parent file that will require
  // a very expensive merge later on.
  return (num_input_files(0) == 1 && num_input_files(1) == 0 &&
          TotalFileSize(grandparents_) <=
              MaxGrandParentOverlapBytes(vset->options_));
}

/**
 * @brief 将compact的文件信息记录到 edit的删除列表中
 * 
 * @param edit 版本增量信息
 */
void Compaction::AddInputDeletions(VersionEdit* edit) {
  for (int which = 0; which < 2; which++) {
    for (size_t i = 0; i < inputs_[which].size(); i++) {
      edit->RemoveFile(level_ + which, inputs_[which][i]->number);
    }
  }
}


/**
 * @brief 如果确定compact后文件落盘到level_+1，该函数判断时候在level_+2及其以后的层里，
 * 是否存在overlap $userKey 的sstable文件。用于判断是否可以删除一个墓碑
 * 
 * @param user_key 
 * @return true 没有overlap $userkey的
 * @return false 
 */
bool Compaction::IsBaseLevelForKey(const Slice& user_key) {
  // Maybe use binary search to find right entry instead of linear search?
  const Comparator* user_cmp = input_version_->vset_->icmp_.user_comparator();
  for (int lvl = level_ + 2; lvl < config::kNumLevels; lvl++) {
    const std::vector<FileMetaData*>& files = input_version_->files_[lvl];
    while (level_ptrs_[lvl] < files.size()) {
      FileMetaData* f = files[level_ptrs_[lvl]];
      if (user_cmp->Compare(user_key, f->largest.user_key()) <= 0) {
        // We've advanced far enough
        if (user_cmp->Compare(user_key, f->smallest.user_key()) >= 0) {
          // Key falls in this file's range, so definitely not base level
          return false;
        }
        break;
      }
      level_ptrs_[lvl]++;
    }
  }
  return true;
}

/**
 * @brief 如果应该停止此次compaction，返回true。用于 DoCompactionWork 函数当中，
 * ShouldStopBefore用于判断是否结束当前输出文件的，比较当前文件已经插入的key和level+2层
 * overlap的文件的总size是否超过阈值（10*2MB），如果超过，就结束文件。
 * 避免这个新的level+1层文件和level+2层重叠的文件数太多，
 * 导致以后该level+1层文件合并到level+2层时牵扯到太多level+2层文件。
 * 
 * @param internal_key the largerst Internal of the range of compaction's output
 * @return true 应该停止此次Compact
 */
bool Compaction::ShouldStopBefore(const Slice& internal_key) {
  const VersionSet* vset = input_version_->vset_;
  // Scan to find earliest grandparent file that contains key.
  const InternalKeyComparator* icmp = &vset->icmp_;
  while (grandparent_index_ < grandparents_.size() &&
         icmp->Compare(internal_key,
                       grandparents_[grandparent_index_]->largest.Encode()) >
             0) {
    if (seen_key_) {
      overlapped_bytes_ += grandparents_[grandparent_index_]->file_size;
    }
    grandparent_index_++;
  }
  seen_key_ = true;

  if (overlapped_bytes_ > MaxGrandParentOverlapBytes(vset->options_)) {
    // Too much overlap for current output; start new output
    overlapped_bytes_ = 0;
    return true;
  } else {
    return false;
  }
}

void Compaction::ReleaseInputs() {
  if (input_version_ != nullptr) {
    input_version_->Unref();
    input_version_ = nullptr;
  }
}

}  // namespace leveldb
