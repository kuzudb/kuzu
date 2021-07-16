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

    bool containVariable(const string& variable) { return variableToDataPosMap.contains(variable); }

    uint64_t getDataChunkPos(const string& variable) {
        GF_ASSERT(variableToDataPosMap.contains(variable));
        return variableToDataPosMap.at(variable).first;
    }

    uint64_t getValueVectorPos(const string& variable) {
        GF_ASSERT(variableToDataPosMap.contains(variable));
        return variableToDataPosMap.at(variable).second;
    }

private:
    // Map each variable to its position pair (dataChunkPos, vectorPos)
    unordered_map<string, pair<uint64_t, uint64_t>> variableToDataPosMap;
};

} // namespace processor
} // namespace graphflow
