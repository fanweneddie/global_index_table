// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "git_iter.h"

namespace leveldb {

GlobalIndex::GitIter::GitIter(std::vector<GITable*> index_files_level0,
                              std::vector<GITable*> index_files_,
                              bool use_file_gran_filter_,
                              VersionSet* vset_) {
  for (auto gitable = index_files_level0.begin();
       gitable != index_files_level0.end(); ++gitable) {
    itrs_.push_back(new GITable::Iterator(*gitable));
  }
  for (auto gitable = index_files_.begin();
       gitable != index_files_.end(); ++gitable) {
    itrs_.push_back(new GITable::Iterator(*gitable));
  }
  this->use_file_gran_filter_ = use_file_gran_filter_;
}

GlobalIndex::GitIter::~GitIter() {
  for (auto itr_itr = itrs_.begin();
       itr_itr != itrs_.end(); ++itr_itr) {
    delete *itr_itr;
  }
}

bool GlobalIndex::GitIter::Valid() const {
  return CurGitIsInBound() && cur_git_ != nullptr && cur_git_->Valid();
}

void GlobalIndex::GitIter::SeekToFirst() {
  // we hope to let cur_git_ point to first gitable
  // and let it seek to first
  if (!itrs_.empty()) {
    cur_git_ = itrs_.front();
    git_index = 0;
    if (cur_git_) {
      cur_git_->SeekToFirst();
    }
  } else {
    cur_git_ = nullptr;
    git_index = -1;
  }
}

void GlobalIndex::GitIter::SeekToLast() {
  // we hope to let cur_git_ point to the last gitable
  // and let it seek to last
  if (!itrs_.empty()) {
    cur_git_ = itrs_.back();
    git_index = itrs_.size() - 1;
    if (cur_git_) {
      cur_git_->SeekToLast();
    }
  } else {
    cur_git_ = nullptr;
    git_index = -1;
  }
}

///// TODO how to return and come in again???
void GlobalIndex::GitIter::Seek(const Slice& target) {
  // the item in gitable to be searched
  SkipListItem search_item = SkipListItem(target);
  // the corresponding node on next level gitable, which can accelerate searching
  GITable::Node* next_level_node = nullptr;
  // search on each level of gitable
  for (auto itr_itr = itrs_.begin(); itr_itr != itrs_.end(); ++itr_itr) {
    // seek on the current gitable
    cur_git_ = *itr_itr;
    cur_git_->SeekToHead();
    cur_git_->SeekWithOrWithoutNode(search_item, next_level_node);
    // check whether the key is definitely not in data block,
    // if so, just return
    if (cur_git_->Valid()) {
      SkipListItem found_item = cur_git_->key();
      next_level_node = (GITable::Node*)found_item.next_level_node;
      kv_maybe_valid = found_item.KeyMaybeInDataBlock(target, use_file_gran_filter_);
      if (kv_maybe_valid) {
        return;
      }
    } else {
      next_level_node = nullptr;
      kv_maybe_valid = false;
    }
  }
}

void GlobalIndex::GitIter::Next() {
  assert(Valid());
  cur_git_->Next();
  if (!Valid()) {
    GotoNextGit();
    if (Valid()) {
      cur_git_->SeekToFirst();
    }
  }
}

void GlobalIndex::GitIter::Prev() {
  assert(Valid());
  cur_git_->Prev();
  if (!Valid()) {
    GotoPrevGit();
    if(Valid()) {
      cur_git_->SeekToLast();
    }
  }
}

bool GlobalIndex::GitIter::CurGitIsInBound() const {
  return git_index >= 0 && git_index < itrs_.size();
}

bool GlobalIndex::GitIter::CurGitHasNext() const {
  return git_index >= -1 && git_index < itrs_.size() - 1;
}

bool GlobalIndex::GitIter::CurGitHasPrev() const {
  return git_index >= 0 && git_index < itrs_.size();
}

void GlobalIndex::GitIter::GotoNextGit() {
  if (CurGitHasNext()) {
    git_index++;
    cur_git_ = itrs_[git_index];
  } else {
    git_index = -1;
    cur_git_ = nullptr;
  }
}

void GlobalIndex::GitIter::GotoPrevGit() {
  if (CurGitHasPrev()) {
    git_index--;
    cur_git_ = itrs_[git_index];
  } else {
    git_index = -1;
    cur_git_ = nullptr;
  }
}

}