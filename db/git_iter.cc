// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_set.h"

namespace leveldb {

// Iterator of the GlobalIndex
class GlobalIndex::GitIter : public Iterator {
 public:
  // Initialize the iterators of each gitable from the GlobalIndex
  GitIter(std::vector<GITable*> index_files_level0,
          std::vector<GITable*> index_files_);

  // Delete the iterators of each gitable
  ~GitIter();

  bool Valid() const override {
    return cur_git_ != nullptr && cur_git_->Valid();
  }

  // Let the current iterator points to the first gitable,
  // and also let it seek to first
  // If there is no gitable, then cur_git_ becomes nullptr
  void SeekToFirst() override;

  // Let the current iterator points to the last gitable
  // and also let it seek to last
  // If there is no gitable, then cur_git_ becomes nullptr
  void SeekToLast() override;

  void Seek(const Slice& target) override;

  void Next() override;

  void Prev() override;

  SkipListItem get_Item() const {
    assert(Valid());
    return cur_git_->key();
  }

  Slice key() const override {
    assert(Valid());
    return cur_git_->key().key;
  }

  Slice value() const override {
    assert(Valid());
    return cur_git_->key().value;
  }

 private:
  // the current iterator
  GITable::Iterator* cur_git_ = nullptr;
  // iterators to gitable on level 0
  std::vector<GITable::Iterator*> itrs_level0_;
  // iterators to gitable on level > 0
  std::vector<GITable::Iterator*> itrs_;
};


GlobalIndex::GitIter::GitIter(std::vector<GITable*> index_files_level0,
        std::vector<GITable*> index_files_) {
  for (auto gitable = index_files_level0.begin();
       gitable != index_files_level0.end(); ++gitable) {
    itrs_level0_.push_back(new GITable::Iterator(*gitable));
  }
  for (auto gitable = index_files_.begin();
       gitable != index_files_.end(); ++gitable) {
    itrs_.push_back(new GITable::Iterator(*gitable));
  }
}

GlobalIndex::GitIter::~GitIter() {
  for (auto itr_itr = itrs_level0_.begin();
       itr_itr != itrs_level0_.end(); ++itr_itr) {
    delete *itr_itr;
  }
  for (auto itr_itr = itrs_.begin();
       itr_itr != itrs_.end(); ++itr_itr) {
    delete *itr_itr;
  }
}

void GlobalIndex::GitIter::SeekToFirst() {
  // we hope to let cur_git_ point to first gitable on level 0
  // or the gitable of level 1
  if (!itrs_level0_.empty()) {
    cur_git_ = itrs_level0_[0];
    if (cur_git_) {
      cur_git_->SeekToFirst();
    }
  } else if (!itrs_.empty()) {
    cur_git_ = itrs_[0];
    if (cur_git_) {
      cur_git_->SeekToFirst();
    }
  } else {
    cur_git_ = nullptr;
  }
}

void GlobalIndex::GitIter::SeekToLast() {
  // we hope to let cur_git_ point to the gitable of last level
  // or the last gitable on level 0
  if (!itrs_.empty()) {
    cur_git_ = itrs_[itrs_.size() - 1];
    if (cur_git_) {
      cur_git_->SeekToLast();
    }
  } else if (!itrs_level0_.empty()) {
    cur_git_ = itrs_level0_[itrs_level0_.size() - 1];
    if (cur_git_) {
      cur_git_->SeekToLast();
    }
  } else {
    cur_git_ = nullptr;
  }
}

}