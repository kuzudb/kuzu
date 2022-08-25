#pragma once

#include "base_logical_operator.h"

#include "src/binder/expression/include/node_expression.h"

namespace graphflow {
namespace planner {
using namespace graphflow::binder;

class LogicalScanNodeID : public LogicalOperator {
public:
    explicit LogicalScanNodeID(shared_ptr<NodeExpression> node) : node{move(node)} {}

    inline LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_SCAN_NODE_ID;
    }

    inline string getExpressionsForPrinting() const override { return node->getRawName(); }

    inline void computeSchema(Schema& schema) {
        auto groupPos = schema.createGroup();
        schema.insertToGroupAndScope(node->getNodeIDPropertyExpression(), groupPos);
    }

    inline shared_ptr<NodeExpression> getNodeExpression() const { return node; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalScanNodeID>(node);
    }

private:
    shared_ptr<NodeExpression> node;
};

} // namespace planner
} // namespace graphflow
