#pragma once

#include <string>
#include <vector>

#include "src/planner/include/logical_plan/operator/logical_operator.h"

using namespace std;

namespace graphflow {
namespace planner {

class LogicalScan : public LogicalOperator {

public:
    LogicalScan(const string& variableName, label_t label)
        : nodeVarName{variableName}, label{label} {}

    LogicalOperatorType getLogicalOperatorType() const { return LogicalOperatorType::LOGICAL_SCAN; }

public:
    const string nodeVarName;
    const label_t label;
};

} // namespace planner
} // namespace graphflow
