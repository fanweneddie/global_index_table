// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef LEVELDB_GIT_MERGING_ITER_H
#define LEVELDB_GIT_MERGING_ITER_H

#include "db/git_iter.h"
#include "db/git_iter.cc"

namespace leveldb {

// Iterator that utilizes gitable and data block.
// Actually, it resembles TwoLevelIterator, but it
// acts the role as MergingIterator.
class GITMergingIter : public Iterator {
 public:
  // Only initialize git_iter, options_ and table_cache_,
  // and let data_iter_ as nullptr.
  GITMergingIter(GITIter* git_iter, ReadOptions options_,
                 TableCache* table_cache_);

  ~GITMergingIter() = default;

  // Check whether current git_iter and data_iter is valid.
  bool Valid() const override;

  // Seek to the first place for both iterators.
  // It's okay if git_iter is not valid,
  // but it does nothing when git_iter is nullptr.
  void SeekToFirst() override;

  // Seek to the last place for both iterators.
  // It's okay if git_iter is not valid,
  // but it does nothing when git_iter is nullptr.
  void SeekToLast() override;

  // Seek the InternalKey as shown in target.
  void Seek(const Slice& target) override;

  // Go to next key.
  void Next() override;

  // Go to previous key.
  void Prev() override;

  // Get the current key
  Slice key() const override;

  // Get the current value
  Slice value() const override;

  // Get the status of current position
  Status status() const override;

 private:
  // initialize the iterator to the current data block.
  void InitDataIterator();
  // simulating TwoLevelIterator
  void SkipEmptyDataBlocksForward();
  void SkipEmptyDataBlocksBackward();

  // the iterator of gitables
  GITIter* git_iter_;
  // the iterator of data blocks
  Iterator* data_iter_;
  // options_ and table_cache_ are used to get data_iter_
  ReadOptions options_;
  TableCache* table_cache_;
};

}

#endif  // LEVELDB_GIT_MERGING_ITER_H