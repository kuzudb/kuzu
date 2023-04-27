#pragma once

#include "base_logical_operator.h"
#include "binder/expression/node_expression.h"

namespace kuzu {
namespace planner {

class LogicalSemiMasker : public LogicalOperator {
public:
    LogicalSemiMasker(std::shared_ptr<binder::NodeExpression> node,
        std::vector<LogicalOperator*> ops, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::SEMI_MASKER, std::move(child)},
          node{std::move(node)}, ops{std::move(ops)} {}

    inline void computeFactorizedSchema() override { copyChildSchema(0); }
    inline void computeFlatSchema() override { copyChildSchema(0); }

    inline std::string getExpressionsForPrinting() const override { return node->toString(); }

    inline std::shared_ptr<binder::NodeExpression> getNode() const { return node; }
    inline std::vector<LogicalOperator*> getOperators() const { return ops; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalSemiMasker>(node, ops, children[0]->copy());
    }

private:
    std::shared_ptr<binder::NodeExpression> node;
    std::vector<LogicalOperator*> ops;
};

} // namespace planner
} // namespace kuzu
