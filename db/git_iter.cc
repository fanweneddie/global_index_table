// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "git_iter.h"

namespace leveldb {

GITIter::GITIter(std::vector<GlobalIndex::GITable*> index_files_level0,
                              std::vector<GlobalIndex::GITable*> index_files_,
                              bool use_file_gran_filter_) {
  for (auto gitable = index_files_level0.begin();
       gitable != index_files_level0.end(); ++gitable) {
    iters_.push_back(new GlobalIndex::GITable::Iterator(*gitable));
  }
  for (auto gitable = index_files_.begin();
       gitable != index_files_.end(); ++gitable) {
    iters_.push_back(new GlobalIndex::GITable::Iterator(*gitable));
  }
  this->use_file_gran_filter_ = use_file_gran_filter_;

}

GITIter::GITIter(GlobalIndex* global_index, bool use_file_gran_filter_) {
  GITIter(global_index->index_files_level0,
          global_index->index_files_,
          use_file_gran_filter_);
}

GITIter::~GITIter() {
  for (auto itr_itr = iters_.begin();
       itr_itr != iters_.end(); ++itr_itr) {
    delete *itr_itr;
  }
}

bool GITIter::Valid() const {
  return CurGitIsInBound() && cur_git_ != nullptr && cur_git_->Valid();
}

void GITIter::SeekToFirst() {
  // we hope to let cur_git_ point to first gitable
  // and let it seek to first
  if (!iters_.empty()) {
    cur_git_ = iters_.front();
    cur_git_index_ = 0;
    if (cur_git_) {
      cur_git_->SeekToFirst();
    }
  } else {
    cur_git_ = nullptr;
    cur_git_index_ = -1;
  }
}

void GITIter::SeekToLast() {
  // we hope to let cur_git_ point to the last gitable
  // and let it seek to last
  if (!iters_.empty()) {
    cur_git_ = iters_.back();
    cur_git_index_ = iters_.size() - 1;
    if (cur_git_) {
      cur_git_->SeekToLast();
    }
  } else {
    cur_git_ = nullptr;
    cur_git_index_ = -1;
  }
}

void GITIter::Seek(const Slice& target) {
  // the item in gitable to be searched
  GlobalIndex::SkipListItem search_item = GlobalIndex::SkipListItem(target);
  // the corresponding node on next level gitable, which can accelerate searching
  GlobalIndex::GITable::Node* next_level_node = nullptr;
  // search on each level of gitable
  for (int i = cur_git_index_ + 1; i < iters_.size(); ++i) {
    // seek on the current gitable
    cur_git_ = iters_[i];
    cur_git_index_++;
    cur_git_->SeekToHead();
    cur_git_->SeekWithOrWithoutNode(search_item, next_level_node);
    // check whether the key is definitely not in data block,
    // if so, just return
    if (cur_git_->Valid()) {
      GlobalIndex::SkipListItem found_item = cur_git_->key();
      next_level_node = (GlobalIndex::GITable::Node*)found_item.next_level_node;
      bool kv_maybe_valid = found_item.KeyMaybeInDataBlock(target, use_file_gran_filter_);
      if (kv_maybe_valid) {
        return;
      }
    } else {
      next_level_node = nullptr;
    }
  }
}

void GITIter::Next() {
  assert(Valid());
  cur_git_->Next();
  if (!Valid()) {
    GotoNextGit();
    if (Valid()) {
      cur_git_->SeekToFirst();
    }
  }
}

void GITIter::Prev() {
  assert(Valid());
  cur_git_->Prev();
  if (!Valid()) {
    GotoPrevGit();
    if(Valid()) {
      cur_git_->SeekToLast();
    }
  }
}

Slice GITIter::key() const {
  assert(Valid());
  return Item().key;
}

Slice GITIter::value() const {
  assert(Valid());
  return Item().value;
}

Status GITIter::status() const {
  if (Valid()) {
    return Status::OK();
  } else {
    return Status::NotFound(Slice("current global index table is invalid."));
  }
}

GlobalIndex::SkipListItem GITIter::Item() const {
  assert(Valid());
  return cur_git_->key();
}

bool GITIter::CurGitIsAtLast() const {
  return cur_git_index_ == iters_.size() - 1;
}

bool GITIter::CurGitIsInBound() const {
  return cur_git_index_ >= 0 && cur_git_index_ < iters_.size();
}

bool GITIter::CurGitHasNext() const {
  return cur_git_index_ >= -1 && cur_git_index_ < iters_.size() - 1;
}

bool GITIter::CurGitHasPrev() const {
  return cur_git_index_ >= 0 && cur_git_index_ < iters_.size();
}

void GITIter::GotoNextGit() {
  if (CurGitHasNext()) {
    cur_git_index_++;
    cur_git_ = iters_[cur_git_index_];
  } else {
    cur_git_index_ = -1;
    cur_git_ = nullptr;
  }
}

void GITIter::GotoPrevGit() {
  if (CurGitHasPrev()) {
    cur_git_index_--;
    cur_git_ = iters_[cur_git_index_];
  } else {
    cur_git_index_ = -1;
    cur_git_ = nullptr;
  }
}

}