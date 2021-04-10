#pragma once

#include <cstdint>

namespace graphflow {
namespace common {

constexpr bool ENABLE_DEBUG = false;
constexpr bool ENABLE_HASH_JOIN_ENUMERATION = false;

// Size (in bytes) of the chunks to be read in GraphLoader.
const uint64_t CSV_READING_BLOCK_SIZE = 1 << 23;

// Size of the page which is the unit of read/write to the files.
// For now, this value cannot be changed. But technically it can change from 2^12 to 2^16. 2^12
// lower bound is assuming the OS page size is 4K. 2^16 is because currently we leave 11 fixed
// number of bits for relOffInPage and the maximum number of bytes needed for an edge is 20 bytes
// so 11 + log_2(20) = 15.xxx, so certainly over 2^16-size pages, we cannot utilize the page for
// storing adjacency lists.
const uint64_t PAGE_SIZE = 1 << 12;

// The default amount of memory pre-allocated to the buffer pool (= 1GB).
const uint64_t DEFAULT_BUFFER_POOL_SIZE = 1ull << 30;

} // namespace common
} // namespace graphflow
