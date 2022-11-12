#pragma once

#include "base_logical_operator.h"
#include "schema.h"

namespace kuzu {
namespace planner {

class LogicalAggregate : public LogicalOperator {
public:
    LogicalAggregate(expression_vector expressionsToGroupBy,
        expression_vector expressionsToAggregate, unique_ptr<Schema> schemaBeforeAggregate,
        shared_ptr<LogicalOperator> child)
        : LogicalOperator{move(child)}, expressionsToGroupBy{move(expressionsToGroupBy)},
          expressionsToAggregate{move(expressionsToAggregate)}, schemaBeforeAggregate{
                                                                    move(schemaBeforeAggregate)} {}

    inline LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_AGGREGATE;
    }

    string getExpressionsForPrinting() const override;

    inline bool hasExpressionsToGroupBy() const { return !expressionsToGroupBy.empty(); }
    inline expression_vector getExpressionsToGroupBy() const { return expressionsToGroupBy; }
    inline expression_vector getExpressionsToAggregate() const { return expressionsToAggregate; }
    inline Schema* getSchemaBeforeAggregate() const { return schemaBeforeAggregate.get(); }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalAggregate>(expressionsToGroupBy, expressionsToAggregate,
            schemaBeforeAggregate->copy(), children[0]->copy());
    }

private:
    expression_vector expressionsToGroupBy;
    expression_vector expressionsToAggregate;
    unique_ptr<Schema> schemaBeforeAggregate;
};

} // namespace planner
} // namespace kuzu
