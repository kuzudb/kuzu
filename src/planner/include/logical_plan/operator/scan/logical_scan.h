#pragma once

#include <string>
#include <vector>

#include "src/planner/include/logical_plan/operator/logical_operator.h"

using namespace std;

namespace graphflow {
namespace planner {

class LogicalScan : public LogicalOperator {

public:
    LogicalScan(const string& variableName, const string& label)
        : nodeVarName{variableName}, label{label} {}

    LogicalOperatorType getLogicalOperatorType() const { return LogicalOperatorType::SCAN; }

public:
    const string nodeVarName;
    const string label;
};

} // namespace planner
} // namespace graphflow
