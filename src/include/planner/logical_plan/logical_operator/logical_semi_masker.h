#pragma once

#include "base_logical_operator.h"
#include "binder/expression/node_expression.h"

namespace kuzu {
namespace planner {
using namespace kuzu::binder;

class LogicalSemiMasker : public LogicalOperator {
public:
    LogicalSemiMasker(shared_ptr<NodeExpression> node, shared_ptr<LogicalOperator> child)
        : LogicalOperator{move(child)}, node{move(node)} {}

    inline LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_SEMI_MASKER;
    }

    inline string getExpressionsForPrinting() const override { return node->getRawName(); }

    inline shared_ptr<NodeExpression> getNode() const { return node; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalSemiMasker>(node, children[0]->copy());
    }

private:
    shared_ptr<NodeExpression> node;
};

} // namespace planner
} // namespace kuzu
