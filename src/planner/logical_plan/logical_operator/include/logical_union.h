#pragma once

#include "base_logical_operator.h"
#include "schema.h"

namespace kuzu {
namespace planner {

class LogicalUnion : public LogicalOperator {
public:
    LogicalUnion(expression_vector expressions, vector<unique_ptr<Schema>> schemasBeforeUnion,
        vector<shared_ptr<LogicalOperator>> children)
        : LogicalOperator{move(children)}, expressionsToUnion{move(expressions)},
          schemasBeforeUnion{move(schemasBeforeUnion)} {}

    inline LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_UNION_ALL;
    }

    string getExpressionsForPrinting() const override;

    inline expression_vector getExpressionsToUnion() { return expressionsToUnion; }

    inline Schema* getSchemaBeforeUnion(uint32_t idx) { return schemasBeforeUnion[idx].get(); }

    inline unique_ptr<LogicalOperator> copy() override {
        vector<unique_ptr<Schema>> copiedSchemas;
        vector<shared_ptr<LogicalOperator>> copiedChildren;
        for (auto i = 0u; i < getNumChildren(); ++i) {
            copiedSchemas.push_back(schemasBeforeUnion[i]->copy());
            copiedChildren.push_back(getChild(i)->copy());
        }
        return make_unique<LogicalUnion>(
            expressionsToUnion, move(copiedSchemas), move(copiedChildren));
    }

private:
    expression_vector expressionsToUnion;
    vector<unique_ptr<Schema>> schemasBeforeUnion;
};

} // namespace planner
} // namespace kuzu
