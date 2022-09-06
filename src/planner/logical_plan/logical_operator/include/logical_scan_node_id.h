#pragma once

#include "base_logical_operator.h"

#include "src/binder/expression/include/node_expression.h"

namespace graphflow {
namespace planner {
using namespace graphflow::binder;

class LogicalScanNodeID : public LogicalOperator {
public:
    explicit LogicalScanNodeID(shared_ptr<NodeExpression> nodeExpression)
        : nodeExpression{move(nodeExpression)}, filter{UINT64_MAX} {}
    LogicalScanNodeID(shared_ptr<NodeExpression> nodeExpression, node_offset_t filter)
        : nodeExpression{move(nodeExpression)}, filter{filter} {}

    inline LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_SCAN_NODE_ID;
    }

    inline string getExpressionsForPrinting() const override {
        return nodeExpression->getRawName();
    }
    inline node_offset_t getFilter() const { return filter; }

    inline shared_ptr<NodeExpression> getNodeExpression() const { return nodeExpression; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalScanNodeID>(nodeExpression);
    }

private:
    shared_ptr<NodeExpression> nodeExpression;
    node_offset_t filter;
};

} // namespace planner
} // namespace graphflow
