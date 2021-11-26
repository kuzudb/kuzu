#pragma once

#include <cstdint>
#include <mutex>

#include "src/common/include/types.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace processor {

struct MorselsDesc {
    mutex mtx;
    const uint64_t maxOffset;
    uint64_t currentOffset;

    explicit MorselsDesc(uint64_t maxOffset) : maxOffset{maxOffset}, currentOffset{0} {}
};

} // namespace processor
} // namespace graphflow
