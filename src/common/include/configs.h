#ifndef GRAPHFLOW_COMMON_STORAGE_CONSTANTS_H_
#define GRAPHFLOW_COMMON_STORAGE_CONSTANTS_H_

#include <cstdint>

namespace graphflow {
namespace common {

const uint64_t CSV_READING_BLOCK_SIZE = 1 << 23;

const uint64_t PAGE_SIZE = 1 << 12;

} // namespace common
} // namespace graphflow

#endif // GRAPHFLOW_COMMON_STORAGE_CONSTANTS_H_
