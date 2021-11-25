#pragma once

#include "src/binder/include/expression/expression.h"
#include "src/planner/include/logical_plan/schema.h"

using namespace graphflow::binder;

namespace graphflow {
namespace planner {

class LogicalAggregate : public LogicalOperator {

public:
    LogicalAggregate(vector<shared_ptr<Expression>> expressionsToGroupBy,
        vector<shared_ptr<Expression>> expressionsToAggregate,
        unique_ptr<Schema> schemaBeforeAggregate, shared_ptr<LogicalOperator> prevOperator)
        : LogicalOperator{move(prevOperator)}, expressionsToGroupBy{move(expressionsToGroupBy)},
          expressionsToAggregate{move(expressionsToAggregate)}, schemaBeforeAggregate{
                                                                    move(schemaBeforeAggregate)} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_AGGREGATE;
    }

    string getExpressionsForPrinting() const override {
        string result = "Group By [";
        for (auto& expression : expressionsToGroupBy) {
            result += expression->getUniqueName() + ", ";
        }
        result += "], Aggregate [";
        for (auto& expression : expressionsToAggregate) {
            result += expression->getUniqueName() + ", ";
        }
        result += "]";
        return result;
    }

    inline vector<shared_ptr<Expression>> getExpressionsToAggregate() const {
        return expressionsToAggregate;
    }
    inline Schema* getSchemaBeforeAggregate() const { return schemaBeforeAggregate.get(); }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalAggregate>(expressionsToGroupBy, expressionsToAggregate,
            schemaBeforeAggregate->copy(), prevOperator->copy());
    }

private:
    vector<shared_ptr<Expression>> expressionsToGroupBy;
    vector<shared_ptr<Expression>> expressionsToAggregate;
    unique_ptr<Schema> schemaBeforeAggregate;
};

} // namespace planner
} // namespace graphflow
