#pragma once

#include "base_logical_operator.h"
#include "binder/expression/node_expression.h"

namespace kuzu {
namespace planner {
using namespace kuzu::binder;

class LogicalScanNode : public LogicalOperator {
public:
    explicit LogicalScanNode(shared_ptr<NodeExpression> node)
        : LogicalOperator{LogicalOperatorType::SCAN_NODE}, node{std::move(node)} {}

    void computeSchema() override;

    inline string getExpressionsForPrinting() const override { return node->getRawName(); }

    inline shared_ptr<NodeExpression> getNode() const { return node; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalScanNode>(node);
    }

private:
    shared_ptr<NodeExpression> node;
};

class LogicalIndexScanNode : public LogicalOperator {
public:
    LogicalIndexScanNode(shared_ptr<NodeExpression> node, shared_ptr<Expression> indexExpression,
        shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::INDEX_SCAN_NODE, std::move(child)},
          node{std::move(node)}, indexExpression{std::move(indexExpression)} {}

    void computeSchema() override;

    inline string getExpressionsForPrinting() const override { return node->getRawName(); }

    inline shared_ptr<NodeExpression> getNode() const { return node; }
    inline shared_ptr<Expression> getIndexExpression() const { return indexExpression; }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalIndexScanNode>(node, indexExpression, children[0]->copy());
    }

private:
    shared_ptr<NodeExpression> node;
    shared_ptr<Expression> indexExpression;
};

} // namespace planner
} // namespace kuzu
