#pragma once

#include "base_logical_operator.h"
#include "schema.h"

namespace kuzu {
namespace planner {

class LogicalDistinct : public LogicalOperator {
public:
    LogicalDistinct(expression_vector expressionsToDistinct, shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::DISTINCT, std::move(child)},
          expressionsToDistinct{std::move(expressionsToDistinct)} {}

    void computeSchema() override;

    string getExpressionsForPrinting() const override;

    inline expression_vector getExpressionsToDistinct() const { return expressionsToDistinct; }
    inline Schema* getSchemaBeforeDistinct() const { return children[0]->getSchema(); }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalDistinct>(expressionsToDistinct, children[0]->copy());
    }

private:
    expression_vector expressionsToDistinct;
};

} // namespace planner
} // namespace kuzu
