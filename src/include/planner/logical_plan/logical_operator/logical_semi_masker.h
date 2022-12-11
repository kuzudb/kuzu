#pragma once

#include "base_logical_operator.h"
#include "binder/expression/node_expression.h"

namespace kuzu {
namespace planner {
using namespace kuzu::binder;

class LogicalSemiMasker : public LogicalOperator {
public:
    LogicalSemiMasker(shared_ptr<NodeExpression> node, shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::SEMI_MASKER, std::move(child)}, node{std::move(
                                                                                   node)} {}

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
