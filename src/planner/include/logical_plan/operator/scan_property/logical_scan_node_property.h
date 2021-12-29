#pragma once

#include "src/planner/include/logical_plan/operator/logical_operator.h"

namespace graphflow {
namespace planner {

class LogicalScanNodeProperty : public LogicalOperator {

public:
    LogicalScanNodeProperty(string nodeID, label_t nodeLabel, string propertyVariableName,
        uint32_t propertyKey, bool isUnstructuredProperty, shared_ptr<LogicalOperator> child)
        : LogicalOperator{move(child)}, nodeID{move(nodeID)}, nodeLabel{nodeLabel},
          propertyVariableName{move(propertyVariableName)}, propertyKey{propertyKey},
          isUnstructuredProperty{isUnstructuredProperty} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_SCAN_NODE_PROPERTY;
    }

    string getExpressionsForPrinting() const override { return propertyVariableName; }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalScanNodeProperty>(nodeID, nodeLabel, propertyVariableName,
            propertyKey, isUnstructuredProperty, children[0]->copy());
    }

public:
    const string nodeID;
    const label_t nodeLabel;
    const string propertyVariableName;
    const uint32_t propertyKey;
    const bool isUnstructuredProperty;
};

} // namespace planner
} // namespace graphflow
