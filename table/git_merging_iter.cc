// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "git_merging_iter.h"
#include "db/table_cache.h"

namespace leveldb {

GITMergingIter::GITMergingIter(GITIter* git_iter, ReadOptions options_,
                               TableCache* table_cache_) {
  this->git_iter_ = git_iter;
  this->data_iter_ = nullptr;
  this->options_ = options_;
  this->table_cache_ = table_cache_;
}

bool GITMergingIter::Valid() const {
  return git_iter_ != nullptr && git_iter_->Valid()
         && data_iter_ != nullptr && data_iter_->Valid();
}

void GITMergingIter::SeekToFirst() {
  if (!git_iter_) {
    return;
  }
  git_iter_->SeekToFirst();
  InitDataIterator();
  if (data_iter_) {
    data_iter_->SeekToFirst();
  }
}

void GITMergingIter::SeekToLast() {
  if (!git_iter_) {
    return;
  }
  git_iter_->SeekToLast();
  InitDataIterator();
  if (data_iter_) {
    data_iter_->SeekToLast();
  }
}

void GITMergingIter::Seek(const Slice& target) {
  if (!git_iter_) {
    return;
  }
  // seek in all gitables until we have found the target
  git_iter_->SeekToFirst();
  while (!git_iter_->CurGitIsAtLast()) {
    git_iter_->Seek(target);
    // we should seek the target in data block
    if (git_iter_->Valid()) {
      InitDataIterator();
      if (data_iter_) {
        data_iter_->Seek(target);
        if (data_iter_->Valid()) {
          break;
        }
      }
    }
  }
}

void GITMergingIter::Next() {
  assert(Valid());
  data_iter_->Next();
  SkipEmptyDataBlocksForward();
}

void GITMergingIter::Prev() {
  assert(Valid());
  data_iter_->Prev();
  SkipEmptyDataBlocksBackward();
}

Slice GITMergingIter::key() const {
  assert(Valid());
  return data_iter_->key();
}

Slice GITMergingIter::value() const {
  assert(Valid());
  return data_iter_->value();
}

Status GITMergingIter::status() const {
  if (!git_iter_|| !git_iter_->Valid()) {
    return Status::NotFound("git_iter_ is not valid.");
  } else if (!data_iter_) {
    return Status::NotFound("data_iter_ is not valid.");
  } else {
    return data_iter_->status();
  }
}

void GITMergingIter::InitDataIterator() {
  if (!git_iter_->Valid()) {
    data_iter_ = nullptr;
  } else {
    GlobalIndex::SkipListItem item = git_iter_->Item();
    table_cache_->GetByIndexBlock(options_, item.file_number,
                                  item.file_size, &data_iter_,
                                  item.value);
  }
}

void GITMergingIter::SkipEmptyDataBlocksForward() {
  while (!data_iter_ || !data_iter_->Valid()) {
    // move to next block
    if (!git_iter_->Valid()) {
      data_iter_ = nullptr;
      return;
    }
    git_iter_->Next();
    InitDataIterator();
    if (!data_iter_) {
      data_iter_->SeekToFirst();
    }
  }
}

void GITMergingIter::SkipEmptyDataBlocksBackward() {
  while (!data_iter_ || !data_iter_->Valid()) {
    // move to previous block
    if (!git_iter_->Valid()) {
      data_iter_ = nullptr;
      return;
    }
    git_iter_->Prev();
    InitDataIterator();
    if (!data_iter_) {
      data_iter_->SeekToLast();
    }
  }
}

Iterator* NewGITMergingIter(GITIter* git_iter, ReadOptions options_,
                            TableCache* table_cache_) {
  return new GITMergingIter(git_iter, options_, table_cache_);
}

}