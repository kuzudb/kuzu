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
        return LogicalOperatorType::HASH_JOIN;
    }

public:
    const string joinNodeVarName;
    const shared_ptr<LogicalOperator> buildSidePrevOperator;
};
} // namespace planner
} // namespace graphflow
