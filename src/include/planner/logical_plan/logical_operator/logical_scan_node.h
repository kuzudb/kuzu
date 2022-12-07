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

class LogicalIndexScanNode : public LogicalOperator {
public:
    LogicalIndexScanNode(shared_ptr<NodeExpression> node, shared_ptr<Expression> indexExpression)
        : LogicalOperator{LogicalOperatorType::INDEX_SCAN_NODE}, node{std::move(node)},
          indexExpression{std::move(indexExpression)} {}

    inline string getExpressionsForPrinting() const override { return node->getRawName(); }

    inline void computeSchema(Schema& schema) {
        auto groupPos = schema.createGroup();
        schema.insertToGroupAndScope(node->getInternalIDProperty(), groupPos);
        schema.setGroupAsSingleState(groupPos);
    }

    inline shared_ptr<NodeExpression> getNode() const { return node; }
    inline shared_ptr<Expression> getIndexExpression() const { return indexExpression; }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalIndexScanNode>(node, indexExpression);
    }

private:
    shared_ptr<NodeExpression> node;
    shared_ptr<Expression> indexExpression;
};

} // namespace planner
} // namespace kuzu
