#pragma once

#include "base_logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalUnion : public LogicalOperator {
public:
    LogicalUnion(expression_vector expressions, vector<shared_ptr<LogicalOperator>> children)
        : LogicalOperator{LogicalOperatorType::UNION_ALL, std::move(children)},
          expressionsToUnion{std::move(expressions)} {}

    void computeSchema() override;

    inline string getExpressionsForPrinting() const override { return string(); }

    inline expression_vector getExpressionsToUnion() { return expressionsToUnion; }

    inline Schema* getSchemaBeforeUnion(uint32_t idx) { return children[idx]->getSchema(); }

    unique_ptr<LogicalOperator> copy() override;

private:
    expression_vector expressionsToUnion;
};

} // namespace planner
} // namespace kuzu
