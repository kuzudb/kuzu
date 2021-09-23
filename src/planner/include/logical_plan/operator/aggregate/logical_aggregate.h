#pragma once

#include "src/binder/include/expression/expression.h"
#include "src/planner/include/logical_plan/schema.h"

using namespace graphflow::binder;

namespace graphflow {
namespace planner {

class LogicalAggregate : public LogicalOperator {

public:
    LogicalAggregate(vector<shared_ptr<Expression>> expressions,
        unique_ptr<Schema> schemaBeforeAggregate, shared_ptr<LogicalOperator> prevOperator)
        : LogicalOperator{move(prevOperator)}, expressionsToAggregate{move(expressions)},
          schemaBeforeAggregate{move(schemaBeforeAggregate)} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_AGGREGATE;
    }

    string getExpressionsForPrinting() const override {
        auto result = expressionsToAggregate[0]->getInternalName();
        for (auto i = 1u; i < expressionsToAggregate.size(); ++i) {
            result += ", " + expressionsToAggregate[i]->getInternalName();
        }
        return result;
    }

    inline vector<shared_ptr<Expression>> getExpressionsToAggregate() const {
        return expressionsToAggregate;
    }
    inline Schema* getSchemaBeforeAggregate() const { return schemaBeforeAggregate.get(); }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalAggregate>(
            expressionsToAggregate, schemaBeforeAggregate->copy(), prevOperator->copy());
    }

private:
    vector<shared_ptr<Expression>> expressionsToAggregate;
    unique_ptr<Schema> schemaBeforeAggregate;
};

} // namespace planner
} // namespace graphflow
