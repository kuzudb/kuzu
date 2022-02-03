#pragma once

#include "src/binder/include/expression/expression.h"
#include "src/planner/include/logical_plan/operator/logical_operator.h"
#include "src/planner/include/logical_plan/schema.h"

using namespace graphflow::binder;

namespace graphflow {
namespace planner {

class LogicalDistinct : public LogicalOperator {

public:
    LogicalDistinct(vector<shared_ptr<Expression>> expressionsToDistinct,
        unique_ptr<Schema> schemaBeforeDistinct, shared_ptr<LogicalOperator> child)
        : LogicalOperator{move(child)}, expressionsToDistinct{move(expressionsToDistinct)},
          schemaBeforeDistinct{move(schemaBeforeDistinct)} {}

    LogicalOperatorType getLogicalOperatorType() const override { return LOGICAL_DISTINCT; }

    string getExpressionsForPrinting() const override {
        string result;
        for (auto& expression : expressionsToDistinct) {
            result += expression->getUniqueName() + ", ";
        }
        return result;
    }

    inline vector<shared_ptr<Expression>> getExpressionsToDistinct() const {
        return expressionsToDistinct;
    }

    inline Schema* getSchemaBeforeDistinct() const { return schemaBeforeDistinct.get(); }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalDistinct>(
            expressionsToDistinct, schemaBeforeDistinct->copy(), children[0]->copy());
    }

private:
    vector<shared_ptr<Expression>> expressionsToDistinct;
    unique_ptr<Schema> schemaBeforeDistinct;
};

} // namespace planner
} // namespace graphflow
