#pragma once

#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_property.h"

namespace graphflow {
namespace planner {

class LogicalScanNodeProperty : public LogicalScanProperty {

public:
    LogicalScanNodeProperty(string nodeID, label_t nodeLabel, string propertyExpressionName,
        uint32_t propertyKey, bool isUnstructuredProperty, shared_ptr<LogicalOperator> child)
        : LogicalScanProperty{move(propertyExpressionName), propertyKey, move(child)},
          nodeID{move(nodeID)}, nodeLabel{nodeLabel}, isUnstructuredProperty{
                                                          isUnstructuredProperty} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_SCAN_NODE_PROPERTY;
    }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalScanNodeProperty>(nodeID, nodeLabel, propertyExpressionName,
            propertyKey, isUnstructuredProperty, children[0]->copy());
    }

public:
    const string nodeID;
    const label_t nodeLabel;
    const bool isUnstructuredProperty;
};

} // namespace planner
} // namespace graphflow
