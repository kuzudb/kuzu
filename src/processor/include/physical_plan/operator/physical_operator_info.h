#pragma once

#include <string>
#include <unordered_map>

#include "src/common/include/assert.h"
#include "src/planner/include/logical_plan/schema.h"
#include "src/processor/include/physical_plan/data_pos.h"

using namespace graphflow::planner;
using namespace std;

namespace graphflow {
namespace processor {

class PhysicalOperatorsInfo {

public:
    explicit PhysicalOperatorsInfo(const Schema& schema);

    inline bool containVariable(const string& variable) const {
        return variableToDataPosMap.contains(variable);
    }

    inline DataPos getDataPos(const string& variable) const {
        GF_ASSERT(variableToDataPosMap.contains(variable));
        return variableToDataPosMap.at(variable);
    }

private:
    // Map each variable to its position pair (dataChunkPos, vectorPos)
    unordered_map<string, DataPos> variableToDataPosMap;
};

} // namespace processor
} // namespace graphflow
