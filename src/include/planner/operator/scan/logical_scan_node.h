#pragma once

#include "binder/expression/node_expression.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalScanNode : public LogicalOperator {
public:
    explicit LogicalScanNode(std::shared_ptr<binder::NodeExpression> node)
        : LogicalOperator{LogicalOperatorType::SCAN_NODE}, node{std::move(node)} {}

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline std::string getExpressionsForPrinting() const override { return node->toString(); }

    inline std::shared_ptr<binder::NodeExpression> getNode() const { return node; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalScanNode>(node);
    }

private:
    std::shared_ptr<binder::NodeExpression> node;
};

} // namespace planner
} // namespace kuzu
