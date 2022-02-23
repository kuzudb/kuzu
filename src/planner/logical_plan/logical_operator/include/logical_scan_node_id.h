#pragma once

#include "base_logical_operator.h"

namespace graphflow {
namespace planner {

class LogicalScanNodeID : public LogicalOperator {

public:
    LogicalScanNodeID(string nodeID, label_t label) : nodeID{move(nodeID)}, label{label} {}

    LogicalOperatorType getLogicalOperatorType() const {
        return LogicalOperatorType::LOGICAL_SCAN_NODE_ID;
    }

    string getExpressionsForPrinting() const override { return nodeID; }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalScanNodeID>(nodeID, label);
    }

public:
    string nodeID;
    label_t label;
};

} // namespace planner
} // namespace graphflow
