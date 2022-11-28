#pragma once

#include "base_logical_operator.h"
#include "binder/expression/node_expression.h"

namespace kuzu {
namespace planner {
using namespace kuzu::binder;

class LogicalScanNode : public LogicalOperator {
public:
    explicit LogicalScanNode(shared_ptr<NodeExpression> node) : node{std::move(node)} {}

    inline LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_SCAN_NODE;
    }

    inline string getExpressionsForPrinting() const override { return node->getRawName(); }

    inline virtual void computeSchema(Schema& schema) {
        auto groupPos = schema.createGroup();
        schema.insertToGroupAndScope(node->getInternalIDProperty(), groupPos);
    }

    inline shared_ptr<NodeExpression> getNode() const { return node; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalScanNode>(node);
    }

protected:
    shared_ptr<NodeExpression> node;
};

class LogicalIndexScanNode : public LogicalScanNode {
public:
    LogicalIndexScanNode(shared_ptr<NodeExpression> node, shared_ptr<Expression> indexExpression)
        : LogicalScanNode{std::move(node)}, indexExpression{std::move(indexExpression)} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_INDEX_SCAN_NODE;
    }

    inline void computeSchema(Schema& schema) override {
        LogicalScanNode::computeSchema(schema);
        auto groupPos = schema.getGroupPos(node->getInternalIDPropertyName());
        schema.getGroup(groupPos)->setIsFlat(true);
    }

    inline shared_ptr<Expression> getIndexExpression() const { return indexExpression; }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalIndexScanNode>(node, indexExpression);
    }

private:
    shared_ptr<Expression> indexExpression;
};

} // namespace planner
} // namespace kuzu
