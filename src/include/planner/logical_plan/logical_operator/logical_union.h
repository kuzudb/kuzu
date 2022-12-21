#pragma once

#include "base_logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalUnion : public LogicalOperator {
public:
    LogicalUnion(expression_vector expressions, vector<unique_ptr<Schema>> schemasBeforeUnion,
        vector<shared_ptr<LogicalOperator>> children)
        : LogicalOperator{LogicalOperatorType::UNION_ALL, std::move(children)},
          expressionsToUnion{std::move(expressions)}, schemasBeforeUnion{
                                                          std::move(schemasBeforeUnion)} {}

    void computeSchema() override;

    inline string getExpressionsForPrinting() const override { return string(); }

    inline expression_vector getExpressionsToUnion() { return expressionsToUnion; }

    inline Schema* getSchemaBeforeUnion(uint32_t idx) { return schemasBeforeUnion[idx].get(); }

    unique_ptr<LogicalOperator> copy() override;

private:
    expression_vector expressionsToUnion;
    vector<unique_ptr<Schema>> schemasBeforeUnion;
};

} // namespace planner
} // namespace kuzu
