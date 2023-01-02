#pragma once

#include "base_logical_operator.h"
#include "binder/expression/node_expression.h"

namespace kuzu {
namespace planner {

class LogicalScanNodeProperty : public LogicalOperator {
public:
    LogicalScanNodeProperty(shared_ptr<NodeExpression> node, expression_vector properties,
        shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::SCAN_NODE_PROPERTY, std::move(child)},
          node{std::move(node)}, properties{std::move(properties)} {}

    void computeSchema() override;

    inline string getExpressionsForPrinting() const override {
        return ExpressionUtil::toString(properties);
    }

    inline shared_ptr<NodeExpression> getNode() const { return node; }
    inline expression_vector getProperties() const { return properties; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalScanNodeProperty>(node, properties, children[0]->copy());
    }

private:
    shared_ptr<NodeExpression> node;
    expression_vector properties;
};

} // namespace planner
} // namespace kuzu
