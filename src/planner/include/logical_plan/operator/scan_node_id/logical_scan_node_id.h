#pragma once

#include <string>
#include <vector>

#include "src/planner/include/logical_plan/operator/logical_operator.h"

using namespace std;

namespace graphflow {
namespace planner {

class LogicalScanNodeID : public LogicalOperator {

public:
    LogicalScanNodeID(const string& variableName, label_t label)
        : nodeVarName{variableName}, label{label} {}

    LogicalOperatorType getLogicalOperatorType() const {
        return LogicalOperatorType::LOGICAL_NODE_ID_SCAN;
    }

    string getOperatorInformation() const override { return nodeVarName; }

public:
    const string nodeVarName;
    const label_t label;
};

} // namespace planner
} // namespace graphflow
