#pragma once

#include <cstdint>
#include <mutex>

#include "src/common/include/file_ser_deser_helper.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace processor {

class PhysicalScan;

class MorselDesc {
    friend class PhysicalScan;

public:
    MorselDesc(const node_offset_t& maxNodeOffset)
        : maxNodeOffset{maxNodeOffset}, currNodeOffset{0} {}

    node_offset_t getCurrNodeOffset() { return currNodeOffset; }

private:
    mutex mtx;
    const node_offset_t maxNodeOffset;
    node_offset_t currNodeOffset;
};

} // namespace processor
} // namespace graphflow
