#pragma once

#include "src/planner/include/logical_plan/operator/logical_operator.h"

namespace graphflow {
namespace planner {

class LogicalScanNodeID : public LogicalOperator {

public:
    LogicalScanNodeID(string nodeID, label_t label) : nodeID{move(nodeID)}, label{label} {}

    LogicalScanNodeID(string nodeID, label_t label, shared_ptr<LogicalOperator> prevOperator)
        : LogicalOperator{prevOperator}, nodeID{move(nodeID)}, label{label} {}

    LogicalOperatorType getLogicalOperatorType() const {
        return LogicalOperatorType::LOGICAL_SCAN_NODE_ID;
    }

    string getExpressionsForPrinting() const override { return nodeID; }

public:
    const string nodeID;
    const label_t label;
};

} // namespace planner
} // namespace graphflow
