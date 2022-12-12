#pragma once

#include "base_logical_operator.h"
#include "schema.h"

namespace kuzu {
namespace planner {

class LogicalDistinct : public LogicalOperator {
public:
    LogicalDistinct(expression_vector expressionsToDistinct,
        unique_ptr<Schema> schemaBeforeDistinct, shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::DISTINCT, std::move(child)},
          expressionsToDistinct{std::move(expressionsToDistinct)}, schemaBeforeDistinct{std::move(
                                                                       schemaBeforeDistinct)} {}

    string getExpressionsForPrinting() const override;

    inline expression_vector getExpressionsToDistinct() const { return expressionsToDistinct; }
    inline Schema* getSchemaBeforeDistinct() const { return schemaBeforeDistinct.get(); }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalDistinct>(
            expressionsToDistinct, schemaBeforeDistinct->copy(), children[0]->copy());
    }

private:
    expression_vector expressionsToDistinct;
    unique_ptr<Schema> schemaBeforeDistinct;
};

} // namespace planner
} // namespace kuzu
