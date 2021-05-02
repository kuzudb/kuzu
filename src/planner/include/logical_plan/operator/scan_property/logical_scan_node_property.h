#pragma once

#include "src/planner/include/logical_plan/operator/logical_operator.h"

namespace graphflow {
namespace planner {

class LogicalScanNodeProperty : public LogicalOperator {

public:
    LogicalScanNodeProperty(string nodeID, label_t nodeLabel, string nodeName, string propertyName,
        shared_ptr<LogicalOperator> prevOperator)
        : LogicalOperator{prevOperator}, nodeID{move(nodeID)}, nodeName{move(nodeName)},
          nodeLabel{nodeLabel}, propertyName{move(propertyName)} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_SCAN_NODE_PROPERTY;
    }

    string getOperatorInformation() const override { return nodeName + "." + propertyName; }

public:
    const string nodeID;
    const string nodeName;
    const label_t nodeLabel;
    const string propertyName;
};

} // namespace planner
} // namespace graphflow
