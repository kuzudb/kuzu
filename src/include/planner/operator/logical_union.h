#pragma once

#include "logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalUnion : public LogicalOperator {
public:
    LogicalUnion(binder::expression_vector expressions,
        std::vector<std::shared_ptr<LogicalOperator>> children)
        : LogicalOperator{LogicalOperatorType::UNION_ALL, std::move(children)},
          expressionsToUnion{std::move(expressions)} {}

    f_group_pos_set getGroupsPosToFlatten(uint32_t childIdx);

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline std::string getExpressionsForPrinting() const override { return std::string{}; }

    inline binder::expression_vector getExpressionsToUnion() { return expressionsToUnion; }

    inline Schema* getSchemaBeforeUnion(uint32_t idx) { return children[idx]->getSchema(); }

    std::unique_ptr<LogicalOperator> copy() override;

private:
    // If an expression to union has different flat/unflat state in different child, we
    // need to flatten that expression in all the single queries.
    bool requireFlatExpression(uint32_t expressionIdx);

private:
    binder::expression_vector expressionsToUnion;
};

} // namespace planner
} // namespace kuzu
