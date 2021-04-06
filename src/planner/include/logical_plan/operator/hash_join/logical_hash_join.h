#pragma once

#include <string>
#include <utility>

#include "src/common/include/types.h"
#include "src/planner/include/logical_plan/operator/logical_operator.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace planner {

class LogicalHashJoin : public LogicalOperator {

public:
    LogicalHashJoin(string joinNodeVarName, shared_ptr<LogicalOperator> buildSidePrevOperator,
        shared_ptr<LogicalOperator> probeSidePrevOperator)
        : LogicalOperator(move(probeSidePrevOperator)), joinNodeVarName(move(joinNodeVarName)),
          buildSidePrevOperator(move(buildSidePrevOperator)) {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_HASH_JOIN;
    }

    string toString(uint64_t depth = 0) const {
        string result = LogicalOperatorTypeNames[getLogicalOperatorType()] + "[" +
                        getOperatorInformation() + "]";
        result += "\nBUILD SIDE: \n";
        result += buildSidePrevOperator->toString(depth + 1);
        result += "\nPROBE SIDE: \n";
        result += prevOperator->toString(depth + 1);
        return result;
    }

    string getOperatorInformation() const override { return joinNodeVarName; }

public:
    const string joinNodeVarName;
    const shared_ptr<LogicalOperator> buildSidePrevOperator;
};
} // namespace planner
} // namespace graphflow
