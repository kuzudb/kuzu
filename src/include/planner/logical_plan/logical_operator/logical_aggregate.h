#pragma once

#include "base_logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalAggregate : public LogicalOperator {
public:
    LogicalAggregate(expression_vector expressionsToGroupBy,
        expression_vector expressionsToAggregate, shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::AGGREGATE, std::move(child)},
          expressionsToGroupBy{std::move(expressionsToGroupBy)}, expressionsToAggregate{std::move(
                                                                     expressionsToAggregate)} {}

    void computeSchema() override;

    string getExpressionsForPrinting() const override;

    inline bool hasExpressionsToGroupBy() const { return !expressionsToGroupBy.empty(); }
    inline expression_vector getExpressionsToGroupBy() const { return expressionsToGroupBy; }
    inline expression_vector getExpressionsToAggregate() const { return expressionsToAggregate; }
    inline Schema* getSchemaBeforeAggregate() const { return children[0]->getSchema(); }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalAggregate>(
            expressionsToGroupBy, expressionsToAggregate, children[0]->copy());
    }

private:
    expression_vector expressionsToGroupBy;
    expression_vector expressionsToAggregate;
};

} // namespace planner
} // namespace kuzu
