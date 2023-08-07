#pragma once

#include "binder/expression/node_expression.h"
#include "planner/logical_plan/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalScanNodeProperty : public LogicalOperator {
public:
    LogicalScanNodeProperty(std::shared_ptr<binder::NodeExpression> node,
        binder::expression_vector properties, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::SCAN_NODE_PROPERTY, std::move(child)},
          node{std::move(node)}, properties{std::move(properties)} {}

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline std::string getExpressionsForPrinting() const override {
        return binder::ExpressionUtil::toString(properties);
    }

    inline std::shared_ptr<binder::NodeExpression> getNode() const { return node; }
    inline binder::expression_vector getProperties() const { return properties; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalScanNodeProperty>(node, properties, children[0]->copy());
    }

private:
    std::shared_ptr<binder::NodeExpression> node;
    binder::expression_vector properties;
};

} // namespace planner
} // namespace kuzu
