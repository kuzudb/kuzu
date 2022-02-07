#pragma once

#include "src/planner/include/logical_plan/operator/logical_operator.h"

using namespace graphflow::binder;

namespace graphflow {
namespace planner {

class LogicalResultCollector : public LogicalOperator {

public:
    LogicalResultCollector(vector<shared_ptr<Expression>> expressionsToCollect,
        unique_ptr<Schema> schema, shared_ptr<LogicalOperator> child)
        : LogicalOperator{move(child)},
          expressionsToCollect{move(expressionsToCollect)}, schema{move(schema)} {}

    inline LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_RESULT_COLLECTOR;
    }

    inline string getExpressionsForPrinting() const override {
        auto result = string();
        for (auto& expression : expressionsToCollect) {
            result += ", " + expression->getUniqueName();
        }
        return result;
    }

    inline vector<shared_ptr<Expression>> getExpressionsToCollect() { return expressionsToCollect; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalResultCollector>(
            expressionsToCollect, schema->copy(), getChild(0)->copy());
    }

    inline Schema* getSchema() { return schema.get(); }

private:
    vector<shared_ptr<Expression>> expressionsToCollect;
    unique_ptr<Schema> schema;
};

} // namespace planner
} // namespace graphflow
