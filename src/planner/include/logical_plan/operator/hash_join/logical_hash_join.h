#pragma once

#include "src/planner/include/logical_plan/operator/logical_operator.h"

namespace graphflow {
namespace planner {

class LogicalHashJoin : public LogicalOperator {

public:
    LogicalHashJoin(string joinNodeID, shared_ptr<LogicalOperator> buildSidePrevOperator,
        shared_ptr<LogicalOperator> probeSidePrevOperator)
        : LogicalOperator(move(probeSidePrevOperator)), joinNodeID(move(joinNodeID)),
          buildSidePrevOperator(move(buildSidePrevOperator)) {}

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
    const shared_ptr<LogicalOperator> buildSidePrevOperator;
};
} // namespace planner
} // namespace graphflow
