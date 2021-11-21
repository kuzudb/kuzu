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
    const label_t label;
    const uint64_t numNodes;
    node_offset_t currNodeOffset;

    MorselsDesc(label_t label, uint64_t numNodes)
        : label{label}, numNodes{numNodes}, currNodeOffset{0} {}
};

} // namespace processor
} // namespace graphflow
