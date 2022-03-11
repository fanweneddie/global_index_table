//
// Created by eddie on 3/11/22.
//

#ifndef LEVELDB_GIT_MERGING_ITER_H
#define LEVELDB_GIT_MERGING_ITER_H

#include "db/git_iter.h"

namespace leveldb {

// Iterator that utilizes gitable and data block.
class GitMergingIter : public Iterator {
 public:
  // Only initialize git_iter and let data_iter_ as nullptr
  GitMergingIter(GitIter* git_iter);

  ~GitMergingIter() = default;

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

  void Seek(const Slice& target) override;

  void Next() override;

  void Prev() override;

 private:
  // initialize the iterator to the current data block.
  void InitDataIterator();

  // the iterator of gitables
  GitIter* git_iter_;
  // the iterator of data blocks
  Iterator* data_iter_;
};

}

#endif  // LEVELDB_GIT_MERGING_ITER_H
