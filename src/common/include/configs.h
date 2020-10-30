#pragma once

#include <cstdint>

namespace graphflow {
namespace common {

// Size (in bytes) of the chunks to be read in GraphLoader.
const uint64_t CSV_READING_BLOCK_SIZE = 1 << 23;

// Size of the page which is the unit of read/write to the files.
const uint64_t PAGE_SIZE = 1 << 12;

// The default amount of memory pre-allocated to the buffer pool (= 128GB).
const uint64_t DEFAULT_BUFFER_POOL_SIZE = 1ull << 37;

} // namespace common
} // namespace graphflow
