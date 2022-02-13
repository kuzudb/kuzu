#pragma once

#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_property.h"

namespace graphflow {
namespace planner {

class LogicalScanNodeProperty : public LogicalScanProperty {

public:
    LogicalScanNodeProperty(string nodeID, label_t nodeLabel, string propertyName,
        uint32_t propertyKey, bool isUnstructured, shared_ptr<LogicalOperator> child)
        : LogicalScanNodeProperty{move(nodeID), nodeLabel, vector<string>{move(propertyName)},
              vector<uint32_t>{propertyKey}, isUnstructured, move(child)} {}

    LogicalScanNodeProperty(string nodeID, label_t nodeLabel, vector<string> propertyNames,
        vector<uint32_t> propertyKeys, bool isUnstructured, shared_ptr<LogicalOperator> child)
        : LogicalScanProperty{move(propertyNames), move(propertyKeys), move(child)},
          nodeID{move(nodeID)}, nodeLabel{nodeLabel}, isUnstructured{isUnstructured} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_SCAN_NODE_PROPERTY;
    }

    inline string getNodeID() const { return nodeID; }

    inline label_t getNodeLabel() const { return nodeLabel; }

    inline bool getIsUnstructured() const { return isUnstructured; }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalScanNodeProperty>(
            nodeID, nodeLabel, propertyNames, propertyKeys, isUnstructured, children[0]->copy());
    }

private:
    string nodeID;
    label_t nodeLabel;
    bool isUnstructured;
};

} // namespace planner
} // namespace graphflow
