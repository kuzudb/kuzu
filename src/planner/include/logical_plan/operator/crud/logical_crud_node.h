#pragma once

#include <unordered_map>

#include "src/planner/include/logical_plan/operator/logical_operator.h"

namespace graphflow {
namespace planner {

class LogicalCRUDNode : public LogicalOperator {
public:
    LogicalCRUDNode(LogicalOperatorType CRUDType, label_t nodeLabel,
        unordered_map<uint32_t, string> propertyKeyToCSVColumnVariableMap,
        shared_ptr<LogicalOperator> prevOperator)
        : LogicalOperator{prevOperator}, CRUDType{CRUDType}, nodeLabel{nodeLabel},
          propertyKeyToCSVColumnVariableMap{propertyKeyToCSVColumnVariableMap} {};

    LogicalOperatorType getLogicalOperatorType() const override { return CRUDType; }

    string getOperatorInformation() const override {
        return LogicalOperatorTypeNames[CRUDType] + ": " + to_string(nodeLabel);
    }

public:
    LogicalOperatorType CRUDType;
    label_t nodeLabel;
    unordered_map<uint32_t, string> propertyKeyToCSVColumnVariableMap;
};

} // namespace planner
} // namespace graphflow
