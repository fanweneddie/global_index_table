// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A filter block is stored near the end of a Table file.  It contains
// filters (e.g., bloom filters) for all data blocks in the table combined
// into a single filter block.

#ifndef STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "leveldb/slice.h"
#include "util/hash.h"

namespace leveldb {

class FilterPolicy;

// A FilterBlockBuilder is used to construct all of the filters for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.
//
// The sequence of calls to FilterBlockBuilder must match the regexp:
//      (StartBlock AddKey*)* Finish
class FilterBlockBuilder {
 public:
  explicit FilterBlockBuilder(const FilterPolicy*);

  FilterBlockBuilder(const FilterBlockBuilder&) = delete;
  FilterBlockBuilder& operator=(const FilterBlockBuilder&) = delete;

  void StartBlock(uint64_t block_offset);
  void AddKey(const Slice& key);
  Slice Finish();

 private:
  void GenerateFilter();

  const FilterPolicy* policy_;
  std::string keys_;             // Flattened key contents
  std::vector<size_t> start_;    // Starting index in keys_ of each key
  std::string result_;           // Filter data computed so far
  std::vector<Slice> tmp_keys_;  // policy_->CreateFilter() argument
  std::vector<uint32_t> filter_offsets_;
};

// Base class of bloom filter reader
// It can be derived by FilterBlockReader and FilterSegmentReader,
// which depends on the granularity of bloom filter (block or segment)
class FilterReader {
 public:
  virtual bool KeyMayMatch(uint64_t block_offset, const Slice& key) const = 0;
};

class FilterBlockReader : public FilterReader {
 public:
  // REQUIRES: "contents" and *policy must stay live while *this is live.
  FilterBlockReader(const FilterPolicy* policy, const Slice& contents);
  bool KeyMayMatch(uint64_t block_offset, const Slice& key) const override;
 private:
  const FilterPolicy* policy_;
  const char* data_;    // Pointer to filter data (at block-start)
  const char* offset_;  // Pointer to beginning of offset array (at block-end)
  size_t num_;          // Number of entries in offset array
  size_t base_lg_;      // Encoding parameter (see kFilterBaseLg in .cc file)

 public:
  const FilterPolicy* getPolicy() const { return policy_; }
  const char* getData() const { return data_; }
  const char* getOffset() const { return offset_; }
  size_t getNum() const { return num_; }
  size_t getBaseLg() const { return base_lg_; };
};

// The reader of a filter segment for a data block
// This data structure is valid only if block_size is bigger than 1 << kFilterBaseLg
// In this case, since a bloom filter segment is generated every 1 << kFilterBaseLg data,
// then a data block can correspond to one bloom filter segment
class FilterSegmentReader : public FilterReader{
 public:
  // Get a bloom filter segment according to the given bloom filter and the block offset
  // REQUIRES: the FilterBlockReader is not null
  // @param fbr: the given bloom filter block
  // @param block_offset: the offset of the data block in the sstable
  FilterSegmentReader(const FilterBlockReader& fbr, uint64_t block_offset);
  // Check whether the given key may be in this bloom filter segment
  // @param block_offset: this parameter is not used
  // @param key: the (internal)key to be searched
  bool KeyMayMatch(uint64_t block_offset, const Slice& key) const override;
 private:
  const FilterPolicy* policy_;
  Slice filter_segment_ = Slice();
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
