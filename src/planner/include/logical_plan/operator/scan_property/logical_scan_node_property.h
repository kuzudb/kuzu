#pragma once

#include <string>

#include "src/planner/include/logical_plan/operator/logical_operator.h"

using namespace std;

namespace graphflow {
namespace planner {

class LogicalScanNodeProperty : public LogicalOperator {

public:
    LogicalScanNodeProperty(const string& nodeVarName, label_t nodeLabel,
        const string& propertyName, shared_ptr<LogicalOperator> prevOperator)
        : LogicalOperator{prevOperator}, nodeVarName{nodeVarName}, nodeLabel{nodeLabel},
          propertyName{propertyName} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_SCAN_NODE_PROPERTY;
    }

    string getOperatorInformation() const override { return nodeVarName + "." + propertyName; }

public:
    const string nodeVarName;
    label_t nodeLabel;
    const string propertyName;
};

} // namespace planner
} // namespace graphflow
