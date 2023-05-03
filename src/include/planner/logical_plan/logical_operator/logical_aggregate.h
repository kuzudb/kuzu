#pragma once

#include "base_logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalAggregate : public LogicalOperator {
public:
    LogicalAggregate(binder::expression_vector expressionsToGroupBy,
        binder::expression_vector expressionsToAggregate, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::AGGREGATE, std::move(child)},
          expressionsToGroupBy{std::move(expressionsToGroupBy)}, expressionsToAggregate{std::move(
                                                                     expressionsToAggregate)} {}

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    f_group_pos_set getGroupsPosToFlattenForGroupBy();
    f_group_pos_set getGroupsPosToFlattenForAggregate();

    std::string getExpressionsForPrinting() const override;

    inline bool hasExpressionsToGroupBy() const { return !expressionsToGroupBy.empty(); }
    inline binder::expression_vector getExpressionsToGroupBy() const {
        return expressionsToGroupBy;
    }
    inline size_t getNumAggregateExpressions() const { return expressionsToAggregate.size(); }
    inline std::shared_ptr<binder::Expression> getAggregateExpression(
        common::vector_idx_t idx) const {
        return expressionsToAggregate[idx];
    }
    inline binder::expression_vector getExpressionsToAggregate() const {
        return expressionsToAggregate;
    }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalAggregate>(
            expressionsToGroupBy, expressionsToAggregate, children[0]->copy());
    }

private:
    bool hasDistinctAggregate();

private:
    binder::expression_vector expressionsToGroupBy;
    binder::expression_vector expressionsToAggregate;
};

} // namespace planner
} // namespace kuzu
