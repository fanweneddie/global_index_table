// Copyright (c) 2022 fanweneddie. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef LEVELDB_GIT_ITER_H
#define LEVELDB_GIT_ITER_H

#include "db/version_set.h"

namespace leveldb {

// An encapsulation of global index table Iterator.
// I build this class because GlobalIndex::GITable::Iterator
// is not a subclass of Iterator, and I don't want to rename any of them.
class GITIter : public Iterator {
 public:
  // Initialize a GITIter from a given global index table
  GITIter(GlobalIndex::GITable* gitable);

  // Delete the iterator
  ~GITIter();

  // Check whether current gitable is valid to be read
  bool Valid() const override;

  // Seek to the first position on current gitable
  void SeekToFirst() override;

  // Seek to the last position on current gitable
  void SeekToLast() override;

  // Seek target in current gitable
  void Seek(const Slice& target) override;

  // Go to next position in current gitable
  void Next() override;

  // Go to previous position in current gitable
  void Prev() override;

  // Get the key (max key) of current item
  // This method is never used (but we still need to implement virtual method key())
  Slice key() const;

  // Get the value (offset) of current item
  // This method is never used (but we still need to implement virtual method value())
  Slice value() const;

  // If cur_git_ is valid, then return OK.
  // Otherwise, return NotFound.
  Status status() const override;

  // Return the item node in the skiplist
  // (we don't use key() and value(), since Item() is a better encapsulation)
  GlobalIndex::SkipListItem Item() const;

 private:
  // iterator to gitable
  GlobalIndex::GITable::Iterator* git_ = nullptr;
};
}

#endif  // LEVELDB_GIT_ITER_H