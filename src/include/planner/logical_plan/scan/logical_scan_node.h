#pragma once

#include "binder/expression/node_expression.h"
#include "planner/logical_plan/logical_operator.h"

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

class LogicalIndexScanNode : public LogicalOperator {
public:
    LogicalIndexScanNode(std::shared_ptr<binder::NodeExpression> node,
        std::shared_ptr<binder::Expression> indexExpression, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::INDEX_SCAN_NODE, std::move(child)},
          node{std::move(node)}, indexExpression{std::move(indexExpression)} {}

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline std::string getExpressionsForPrinting() const override { return node->toString(); }

    inline std::shared_ptr<binder::NodeExpression> getNode() const { return node; }
    inline std::shared_ptr<binder::Expression> getIndexExpression() const {
        return indexExpression;
    }

    std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalIndexScanNode>(node, indexExpression, children[0]->copy());
    }

private:
    std::shared_ptr<binder::NodeExpression> node;
    std::shared_ptr<binder::Expression> indexExpression;
};

} // namespace planner
} // namespace kuzu
