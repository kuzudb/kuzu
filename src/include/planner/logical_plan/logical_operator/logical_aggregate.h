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

    void computeSchema() override;

    std::string getExpressionsForPrinting() const override;

    inline bool hasExpressionsToGroupBy() const { return !expressionsToGroupBy.empty(); }
    inline binder::expression_vector getExpressionsToGroupBy() const {
        return expressionsToGroupBy;
    }
    inline binder::expression_vector getExpressionsToAggregate() const {
        return expressionsToAggregate;
    }
    inline Schema* getSchemaBeforeAggregate() const { return children[0]->getSchema(); }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalAggregate>(
            expressionsToGroupBy, expressionsToAggregate, children[0]->copy());
    }

private:
    binder::expression_vector expressionsToGroupBy;
    binder::expression_vector expressionsToAggregate;
};

class LogicalAggregateFactorizationSolver {
public:
    static std::unordered_set<f_group_pos> getGroupsPosToFlattenForGroupBy(
        LogicalAggregate* aggregate) {
        return getGroupsPosToFlattenForGroupBy(aggregate->getExpressionsToGroupBy(),
            aggregate->getExpressionsToAggregate(), aggregate->getChild(0).get());
    }
    static std::unordered_set<f_group_pos> getGroupsPosToFlattenForAggregate(
        LogicalAggregate* aggregate) {
        return getGroupsPosToFlattenForAggregate(
            aggregate->getExpressionsToAggregate(), aggregate->getChild(0).get());
    }
    static std::unordered_set<f_group_pos> getGroupsPosToFlattenForGroupBy(
        const binder::expression_vector& expressionsToGroupBy,
        const binder::expression_vector& expressionsToAggregate, LogicalOperator* aggregateChild);
    static std::unordered_set<f_group_pos> getGroupsPosToFlattenForAggregate(
        const binder::expression_vector& expressionsToAggregate, LogicalOperator* aggregateChild);

private:
    static bool hasDistinctAggregate(const binder::expression_vector& expressionsToAggregate);
};

} // namespace planner
} // namespace kuzu
