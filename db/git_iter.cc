// Copyright (c) 2022 fanweneddie. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "git_iter.h"

namespace leveldb {

GITIter::GITIter(GlobalIndex::GITable* gitable) {
  this->git_ = new GlobalIndex::GITable::Iterator(gitable);
}

GITIter::~GITIter() {
  delete git_;
}

bool GITIter::Valid() const {
  return git_ != nullptr && git_->Valid();
}

void GITIter::SeekToFirst() {
  if (git_) {
    git_->SeekToFirst();
  }
}

void GITIter::SeekToLast() {
  if (git_) {
    git_->SeekToLast();
  }
}

void GITIter::Seek(const Slice& target) {
  if (git_) {
    git_->Seek(target);
  }
}

void GITIter::Next() {
  assert(Valid());
  git_->Next();
}

void GITIter::Prev() {
  assert(Valid());
  git_->Prev();
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
  return git_->key();
}

}