#pragma once

#include <cassert>
#include <cstdint>
#include <unordered_set>
#include <utility>
#include <vector>

#include "src/common/types/include/types_include.h"

namespace graphflow {
namespace common {

class NodeIDCompressionScheme {

public:
    NodeIDCompressionScheme(const unordered_set<table_id_t>& nbrNodeTableIDs,
        const vector<node_offset_t>& maxNodeOffsetsPerTable);
    NodeIDCompressionScheme() : NodeIDCompressionScheme(0, 8){};

private:
    NodeIDCompressionScheme(uint32_t numBytesForTableID, uint32_t numBytesForOffset)
        : numBytesForTableID(numBytesForTableID), numBytesForOffset(numBytesForOffset),
          commonTableID(UINT64_MAX) {}

public:
    inline uint32_t getNumBytesForTableID() const { return numBytesForTableID; };
    inline uint32_t getNumBytesForOffset() const { return numBytesForOffset; };
    inline uint32_t getTotalNumBytes() const { return numBytesForTableID + numBytesForOffset; };
    inline table_id_t getCommonTableID() const { return commonTableID; };

private:
    static uint32_t getNumBytesForEncoding(uint64_t maxValToEncode, uint8_t minNumBytes);

private:
    uint32_t numBytesForTableID;
    uint32_t numBytesForOffset;
    table_id_t commonTableID;
};

} // namespace common
} // namespace graphflow
