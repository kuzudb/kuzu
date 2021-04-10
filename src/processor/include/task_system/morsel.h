#pragma once

#include <cstdint>
#include <mutex>

#include "src/common/include/file_ser_deser_helper.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace processor {

struct MorselsDesc {
    mutex mtx;
    const uint64_t numNodes;
    node_offset_t currNodeOffset;

    MorselsDesc(uint64_t numNodes) : numNodes{numNodes}, currNodeOffset{0} {}
};

} // namespace processor
} // namespace graphflow
