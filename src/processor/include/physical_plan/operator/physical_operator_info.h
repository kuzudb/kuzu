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

    inline DataPos getDataPos(const string& name) const {
        GF_ASSERT(expressionNameToDataPosMap.contains(name));
        return expressionNameToDataPosMap.at(name);
    }

    inline void addComputedExpressions(const string& name) { computedExpressionNames.insert(name); }

    inline bool expressionHasComputed(const string& name) const {
        return computedExpressionNames.contains(name);
    }

private:
    // Map each variable to its position pair (dataChunkPos, vectorPos)
    unordered_map<string, DataPos> expressionNameToDataPosMap;
    unordered_set<string> computedExpressionNames;
};

} // namespace processor
} // namespace graphflow
