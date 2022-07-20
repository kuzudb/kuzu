#pragma once

#include "base_logical_operator.h"

#include "src/binder/expression/include/node_expression.h"

namespace graphflow {
namespace planner {
using namespace graphflow::binder;

class LogicalScanNodeID : public LogicalOperator {
public:
    LogicalScanNodeID(shared_ptr<NodeExpression> nodeExpression)
        : nodeExpression{move(nodeExpression)} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_SCAN_NODE_ID;
    }

    string getExpressionsForPrinting() const override { return nodeExpression->getRawName(); }

    inline shared_ptr<NodeExpression> getNodeExpression() const { return nodeExpression; }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalScanNodeID>(nodeExpression);
    }

private:
    shared_ptr<NodeExpression> nodeExpression;
};

} // namespace planner
} // namespace graphflow
