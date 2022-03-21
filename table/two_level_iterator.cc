// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/two_level_iterator.h"
#include "leveldb/table.h"
#include "db/git_iter.h"
#include "db/table_cache.h"

namespace leveldb {

TwoLevelIterator::TwoLevelIterator(Iterator* index_iter,
                                   BlockFunction block_function, void* arg,
                                   const ReadOptions& options)
    : block_function_(block_function),
      arg_(arg),
      options_(options),
      index_iter_(index_iter),
      data_iter_(nullptr) {}

TwoLevelIterator::~TwoLevelIterator() = default;

void TwoLevelIterator::Seek(const Slice& target) {
  index_iter_.Seek(target);
  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.Seek(target);
  SkipEmptyDataBlocksForward();
}

bool TwoLevelIterator::SeekWithOrWithoutNode(const Slice& target, void* node) {
  // for git, first find the position of node
  if (UseGit() && node) {
    GlobalIndex::SkipListItem* item = (GlobalIndex::SkipListItem*)node;
    index_iter_.Seek(item->key);
  }
  // seek the target in index block or git
  index_iter_.Seek(target);
  // check bloom filter
  if (!index_iter_.Valid()) {
    return false;
  } else if (UseGit()) {
    GlobalIndex::SkipListItem item = dynamic_cast<GITIter*>(index_iter_.iter())->Item();
    bool kv_maybe_valid = item.KeyMaybeInDataBlock(target,
                                                   options_.useFileGranFilter());
    if (kv_maybe_valid) {
      return false;
    }
  }
  // load data block
  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.Seek(target);
  SkipEmptyDataBlocksForward();
  return true;
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

bool TwoLevelIterator::UseGit() const {
  if (dynamic_cast<GITIter*>(index_iter_.iter())) {
    return true;
  } else {
    return false;
  }
}

IteratorWrapper TwoLevelIterator::Get_index_iter() const {
  return index_iter_;
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
    // the offset to the data block
    Slice handle = index_iter_.value();
    if (data_iter_.iter() != nullptr &&
        handle.compare(data_block_handle_) == 0) {
      // data_iter_ is already constructed with this iterator, so
      // no need to change anything
    } else {
      Iterator* iter;
      // 1. get data block iterator for gitable
      if (UseGit()) {
        GlobalIndex::SkipListItem item = (dynamic_cast<GITIter*>(index_iter_.iter()))->Item();
        TableCache* table_cache = (TableCache*)arg_;
        table_cache->GetByIndexBlock(options_, item.file_number,
                                      item.file_size, &iter,
                                      item.value);
      }
      // 2. get data block iterator for index block
      else {
        iter = (*block_function_)(arg_, options_, handle);
      }
      data_block_handle_.assign(handle.data(), handle.size());
      SetDataIterator(iter);
    }
  }
}

Iterator* NewTwoLevelIterator(Iterator* index_iter,
                              BlockFunction block_function, void* arg,
                              const ReadOptions& options) {
  return new TwoLevelIterator(index_iter, block_function, arg, options);
}

}  // namespace leveldb