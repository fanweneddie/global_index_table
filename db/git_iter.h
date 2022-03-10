//
// Created by eddie on 3/10/22.
//

#ifndef LEVELDB_GIT_ITER_H
#define LEVELDB_GIT_ITER_H

#include "db/version_set.h"

namespace leveldb {

// Iterator of the GlobalIndex
class GlobalIndex::GitIter : public Iterator {
 public:
  // Initialize the iterators of each gitable from the GlobalIndex
  GitIter(std::vector<GITable*> index_files_level0,
          std::vector<GITable*> index_files_,
          bool use_file_gran_filter_, VersionSet* vset_);

  // Delete the iterators of each gitable
  ~GitIter();

  // Check whether current gitable is valid to be read
  bool Valid() const override;

  // Let the current iterator points to the first gitable,
  // and also let it seek to first
  // If there is no gitable, then cur_git_ becomes nullptr
  void SeekToFirst() override;

  // Let the current iterator points to the last gitable
  // and also let it seek to last
  // If there is no gitable, then cur_git_ becomes nullptr
  void SeekToLast() override;

  // Seek the first node that target maybe in (after the current gitable)
  void Seek(const Slice& target) override;

  // If current node is not the last one, go to next node in current gitable.
  // Else, go to the first node in next gitable
  void Next() override;

  // If current node is not the first one, go to previous node in current gitable.
  // Else, go to the last node in previous gitable
  void Prev() override;

  // Return the item node in the skiplist
  // (we don't use key() and value(), since Item() is a better encapsulation)
  SkipListItem Item() const  {
    assert(Valid() && kv_maybe_valid);
    return cur_git_->key();
  }

 private:
  // Check whether current gitable's index is in bound
  bool CurGitIsInBound() const;

  // Check whether current gitable has its next gitable
  bool CurGitHasNext() const;

  // Check whether current gitable has its previous gitable
  bool CurGitHasPrev() const;

  // Go to next gitable
  void GotoNextGit();

  // Go to previous gitable
  void GotoPrevGit();

  // the index of current iterator in itrs_
  // -1 means that the iterator of current gitable is invalid
  int git_index = -1;
  GITable::Iterator* cur_git_ = nullptr;
  // iterators to gitable
  std::vector<GITable::Iterator*> itrs_;
  // whether a filter manages a file or a data block
  bool use_file_gran_filter_ = true;
  // whether current key and value may be what we want
  bool kv_maybe_valid = false;
};
}

#endif  // LEVELDB_GIT_ITER_H
