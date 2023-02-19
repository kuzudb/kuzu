#pragma once

#include "base_logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalUnion : public LogicalOperator {
public:
    LogicalUnion(binder::expression_vector expressions,
        std::vector<std::shared_ptr<LogicalOperator>> children)
        : LogicalOperator{LogicalOperatorType::UNION_ALL, std::move(children)},
          expressionsToUnion{std::move(expressions)} {}

    void computeSchema() override;

    inline std::string getExpressionsForPrinting() const override { return std::string(); }

    inline binder::expression_vector getExpressionsToUnion() { return expressionsToUnion; }

    inline Schema* getSchemaBeforeUnion(uint32_t idx) { return children[idx]->getSchema(); }

    std::unique_ptr<LogicalOperator> copy() override;

private:
    binder::expression_vector expressionsToUnion;
};

class LogicalUnionFactorizationSolver {
public:
    static std::unordered_set<f_group_pos> getGroupsPosToFlatten(uint32_t numExpressions,
        LogicalOperator* unionChild,
        const std::vector<std::shared_ptr<LogicalOperator>>& unionChildren);

private:
    // If an expression to union has different flat/unflat state in different child, we
    // need to flatten that expression in all the single queries.
    static bool requireFlatExpression(
        uint32_t expressionIdx, const std::vector<std::shared_ptr<LogicalOperator>>& unionChildren);
};

} // namespace planner
} // namespace kuzu
