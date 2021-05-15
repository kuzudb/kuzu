#pragma once

#include "src/planner/include/logical_plan/operator/logical_operator.h"

namespace graphflow {
namespace planner {

class LogicalCreateNode : public LogicalOperator {
public:
    LogicalCreateNode(label_t nodeLabel,
        unordered_map<uint32_t, string> propertyKeyToCSVColumnVariableMap,
        shared_ptr<LogicalOperator> prevOperator)
        : LogicalOperator{prevOperator}, nodeLabel{nodeLabel},
          propertyKeyToCSVColumnVariableMap{propertyKeyToCSVColumnVariableMap} {};

    string getOperatorInformation() const override { return to_string(nodeLabel); }

public:
    label_t nodeLabel;
    unordered_map<uint32_t, string> propertyKeyToCSVColumnVariableMap;
};

} // namespace planner
} // namespace graphflow
