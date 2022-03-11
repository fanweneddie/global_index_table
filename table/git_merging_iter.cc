// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "git_merging_iter.h"

namespace leveldb {

GitMergingIter::GitMergingIter(GitIter* git_iter) {
  this->git_iter_ = git_iter;
  this->data_iter_ = nullptr;
}

bool GitMergingIter::Valid() const {
  return git_iter_ != nullptr && git_iter_->Valid()
        && data_iter_ != nullptr && data_iter_->Valid();
}

void GitMergingIter::SeekToFirst() {
  if (!git_iter_) {
    return;
  }
  git_iter_->SeekToFirst();
  InitDataIterator();
  if (data_iter_) {
    data_iter_->SeekToFirst();
  }
}

void GitMergingIter::SeekToLast() {
  if (!git_iter_) {
    return;
  }
  git_iter_->SeekToLast();
  InitDataIterator();
  if (data_iter_) {
    data_iter_->SeekToFirst();
  }
}

void GitMergingIter::InitDataIterator() {

}

}