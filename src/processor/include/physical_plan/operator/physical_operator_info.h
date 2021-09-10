#pragma once

#include <string>
#include <unordered_map>

#include "src/common/include/assert.h"
#include "src/planner/include/logical_plan/schema.h"

using namespace graphflow::planner;
using namespace std;

namespace graphflow {
namespace processor {

class PhysicalOperatorsInfo {

public:
    explicit PhysicalOperatorsInfo(const Schema& schema);

    inline bool containVariable(const string& variable) {
        return variableToDataPosMap.contains(variable);
    }

    inline pair<uint32_t, uint32_t> getDataChunkAndValueVectorPos(const string& variable) {
        GF_ASSERT(variableToDataPosMap.contains(variable));
        return variableToDataPosMap.at(variable);
    }

    inline uint32_t getDataChunkPos(const string& variable) {
        return getDataChunkAndValueVectorPos(variable).first;
    }

    inline uint32_t getTotalNumDataChunks() { return totalNumDataChunks; }

    inline uint32_t getDataChunkSize(uint32_t dataChunkPos) {
        GF_ASSERT(dataChunkPosToNumValueVectors.contains(dataChunkPos));
        return dataChunkPosToNumValueVectors.at(dataChunkPos);
    }

private:
    // Map each variable to its position pair (dataChunkPos, vectorPos)
    unordered_map<string, pair<uint32_t, uint32_t>> variableToDataPosMap;

    uint32_t totalNumDataChunks;
    unordered_map<uint32_t, uint32_t> dataChunkPosToNumValueVectors;
};

} // namespace processor
} // namespace graphflow
