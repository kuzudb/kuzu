#pragma once

#include "src/planner/include/logical_plan/operator/logical_operator.h"
#include "src/planner/include/logical_plan/schema.h"

namespace graphflow {
namespace planner {

class LogicalHashJoin : public LogicalOperator {

public:
    LogicalHashJoin(string joinNodeID, shared_ptr<LogicalOperator> buildSidePrevOperator,
        unique_ptr<Schema> buildSideSchema, uint32_t probeSideFlatGroupPos,
        vector<uint32_t> probeSideUnFlatGroupsPos,
        shared_ptr<LogicalOperator> probeSidePrevOperator)
        : LogicalOperator(move(probeSidePrevOperator)),
          joinNodeID(move(joinNodeID)), buildSidePrevOperator{move(buildSidePrevOperator)},
          buildSideSchema(move(buildSideSchema)), probeSideFlatGroupPos{probeSideFlatGroupPos},
          probeSideUnFlatGroupsPos{move(probeSideUnFlatGroupsPos)} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_HASH_JOIN;
    }

    string toString(uint64_t depth = 0) const {
        string result = LogicalOperatorTypeNames[getLogicalOperatorType()] + "[" +
                        getExpressionsForPrinting() + "]";
        result += "\nBUILD SIDE: \n";
        result += buildSidePrevOperator->toString(depth + 1);
        result += "\nPROBE SIDE: \n";
        result += prevOperator->toString(depth + 1);
        return result;
    }

    string getExpressionsForPrinting() const override { return joinNodeID; }

public:
    const string joinNodeID;
    shared_ptr<LogicalOperator> buildSidePrevOperator;
    unique_ptr<Schema> buildSideSchema;
    uint32_t probeSideFlatGroupPos;
    vector<uint32_t> probeSideUnFlatGroupsPos;
};
} // namespace planner
} // namespace graphflow
