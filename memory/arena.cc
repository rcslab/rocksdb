//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <sys/param.h>
#include <fcntl.h>
#include <sls_wal.h>
#include <stdio.h>
#include <time.h>

#include "memory/arena.h"
#ifndef OS_WIN
#include <sys/mman.h>
#endif
#include <algorithm>
#include "logging/logging.h"
#include "port/malloc.h"
#include "port/port.h"
#include "rocksdb/env.h"
#include "test_util/sync_point.h"

namespace ROCKSDB_NAMESPACE {

// MSVC complains that it is already defined since it is static in the header.
#ifndef _MSC_VER
const size_t Arena::kInlineSize;
#endif

std::atomic<uint64_t> Arena::mapping_target_(0x700000000000);
const size_t Arena::kMinBlockSize = 4096;
const size_t Arena::kMaxBlockSize = 2u << 30;
static const int kAlignUnit = alignof(max_align_t);

size_t OptimizeBlockSize(size_t block_size) {
  // Make sure block_size is in optimal range
  block_size = std::max(Arena::kMinBlockSize, block_size);
  block_size = std::min(Arena::kMaxBlockSize, block_size);

  // make sure block_size is the multiple of kAlignUnit
  if (block_size % kAlignUnit != 0) {
    block_size = (1 + block_size / kAlignUnit) * kAlignUnit;
  }

  return block_size;
}

Arena::Arena(size_t block_size, AllocTracker* tracker, size_t huge_page_size)
    : kBlockSize(OptimizeBlockSize(block_size)), tracker_(tracker) {
  assert(kBlockSize >= kMinBlockSize && kBlockSize <= kMaxBlockSize &&
         kBlockSize % kAlignUnit == 0);
  TEST_SYNC_POINT_CALLBACK("Arena::Arena:0", const_cast<size_t*>(&kBlockSize));
  alloc_bytes_remaining_ = sizeof(inline_block_);
  blocks_memory_ += alloc_bytes_remaining_;
  aligned_alloc_ptr_ = inline_block_;
  unaligned_alloc_ptr_ = inline_block_ + alloc_bytes_remaining_;
#ifdef MAP_HUGETLB
  hugetlb_size_ = huge_page_size;
  if (hugetlb_size_ && kBlockSize > hugetlb_size_) {
    hugetlb_size_ = ((kBlockSize - 1U) / hugetlb_size_ + 1U) * hugetlb_size_;
  }
#else
  (void)huge_page_size;
#endif
  if (tracker_ != nullptr) {
    tracker_->Allocate(kInlineSize);
  }
}

Arena::~Arena() {
  if (tracker_ != nullptr) {
    assert(tracker_->is_freed());
    tracker_->FreeMem();
  }
  for (const auto& mmap_info : blocks_) {
    if (mmap_info.addr_ == nullptr) {
      continue;
    }
    int ret = munmap(mmap_info.addr_, mmap_info.length_);
    if (ret != 0) {
      // TODO(sdong): Better handling
    }
  }

#ifdef MAP_HUGETLB
  for (const auto& mmap_info : huge_blocks_) {
    if (mmap_info.addr_ == nullptr) {
      continue;
    }
    auto ret = munmap(mmap_info.addr_, mmap_info.length_);
    if (ret != 0) {
      // TODO(sdong): Better handling
    }
  }
#endif
}

char* Arena::AllocateFallback(size_t bytes, bool aligned) {
  if (bytes > kBlockSize / 4) {
    ++irregular_block_num;
    // Object is more than a quarter of our block size.  Allocate it separately
    // to avoid wasting too much space in leftover bytes.
    return AllocateRegion(bytes);
  }

  // We waste the remaining space in the current block.
  size_t size = 0;
  char* block_head = nullptr;
#ifdef MAP_HUGETLB
  if (hugetlb_size_) {
    size = hugetlb_size_;
    block_head = AllocateFromHugePage(size);
  }
#endif
  if (!block_head) {
    size = kBlockSize;
    block_head = AllocateRegion(size);
  }
  alloc_bytes_remaining_ = size - bytes;

  if (aligned) {
    aligned_alloc_ptr_ = block_head + bytes;
    unaligned_alloc_ptr_ = block_head + size;
    return block_head;
  } else {
    aligned_alloc_ptr_ = block_head;
    unaligned_alloc_ptr_ = block_head + size - bytes;
    return unaligned_alloc_ptr_;
  }
}

char* Arena::AllocateFromHugePage(size_t bytes) {
#ifdef MAP_HUGETLB
  if (hugetlb_size_ == 0) {
    return nullptr;
  }
  // Reserve space in `huge_blocks_` before calling `mmap`.
  // Use `emplace_back()` instead of `reserve()` to let std::vector manage its
  // own memory and do fewer reallocations.
  //
  // - If `emplace_back` throws, no memory leaks because we haven't called
  //   `mmap` yet.
  // - If `mmap` throws, no memory leaks because the vector will be cleaned up
  //   via RAII.
  huge_blocks_.emplace_back(nullptr /* addr */, 0 /* length */);

  void* addr = mmap(nullptr, bytes, (PROT_READ | PROT_WRITE),
                    (MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB), -1, 0);

  if (addr == MAP_FAILED) {
    return nullptr;
  }
  huge_blocks_.back() = MmapInfo(addr, bytes);
  blocks_memory_ += bytes;
  if (tracker_ != nullptr) {
    tracker_->Allocate(bytes);
  }
  return reinterpret_cast<char*>(addr);
#else
  (void)bytes;
  return nullptr;
#endif
}

char* Arena::AllocateRegion(size_t bytes) {
  // Reserve space in `blocks_` before calling `mmap`.
  // Use `emplace_back()` instead of `reserve()` to let std::vector manage its
  // own memory and do fewer reallocations.
  //
  // - If `emplace_back` throws, no memory leaks because we haven't called
  //   `mmap` yet.
  // - If `mmap` throws, no memory leaks because the vector will be cleaned up
  //   via RAII.
  blocks_.emplace_back(nullptr /* addr */, 0 /* length */);
if (bytes % PAGE_SIZE != 0)
    bytes = bytes - (bytes % PAGE_SIZE) + PAGE_SIZE;

  struct timespec ts;
  clock_gettime(CLOCK_REALTIME_FAST, &ts);
  uint64_t nanosecs = (1000 * 1000 * 1000 * ts.tv_sec) + ts.tv_nsec;

  char filename[1024];
  snprintf(filename, 1024, "/tmp/sas-%ld", nanosecs);

  int error = slsfs_sas_create(filename, 10ULL * 1024 * 1024 * 1024);
  if (error != 0) {
	  printf("SAS creation failed\n");
	  throw 42;
  }

  int fd = open(filename, O_RDWR, 0666);
  if (fd < 0) {
	  perror("open");
	  throw 42;
  }

  void *addr;
  error = slsfs_sas_map(fd, &addr);
  if (error != 0) {
	  printf("slsfs_sas_map failed %d\n", error);
	  throw 42;
  }

  blocks_.back() = MmapInfo(addr, bytes);
  blocks_memory_ += bytes;
  if (tracker_ != nullptr) {
    tracker_->Allocate(bytes);
  }
  return reinterpret_cast<char*>(addr);
}

char* Arena::AllocateAligned(size_t bytes, size_t huge_page_size,
                             Logger* logger) {
  assert((kAlignUnit & (kAlignUnit - 1)) ==
         0);  // Pointer size should be a power of 2

#ifdef MAP_HUGETLB
  if (huge_page_size > 0 && bytes > 0) {
    // Allocate from a huge page TBL table.
    assert(logger != nullptr);  // logger need to be passed in.
    size_t reserved_size =
        ((bytes - 1U) / huge_page_size + 1U) * huge_page_size;
    assert(reserved_size >= bytes);

    char* addr = AllocateFromHugePage(reserved_size);
    if (addr == nullptr) {
      ROCKS_LOG_WARN(logger,
                     "AllocateAligned fail to allocate huge TLB pages: %s",
                     strerror(errno));
      // fail back to malloc
    } else {
      return addr;
    }
  }
#else
  (void)huge_page_size;
  (void)logger;
#endif

  size_t current_mod =
      reinterpret_cast<uintptr_t>(aligned_alloc_ptr_) & (kAlignUnit - 1);
  size_t slop = (current_mod == 0 ? 0 : kAlignUnit - current_mod);
  size_t needed = bytes + slop;
  char* result;
  if (needed <= alloc_bytes_remaining_) {
    result = aligned_alloc_ptr_ + slop;
    aligned_alloc_ptr_ += needed;
    alloc_bytes_remaining_ -= needed;
  } else {
    // AllocateFallback always returns aligned memory
    result = AllocateFallback(bytes, true /* aligned */);
  }
  assert((reinterpret_cast<uintptr_t>(result) & (kAlignUnit - 1)) == 0);
  return result;
}

}  // namespace ROCKSDB_NAMESPACE
