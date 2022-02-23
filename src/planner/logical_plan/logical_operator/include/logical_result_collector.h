#pragma once

#include "base_logical_operator.h"
#include "schema.h"

#include "src/binder/expression/include/expression.h"

using namespace graphflow::binder;

namespace graphflow {
namespace planner {

class LogicalResultCollector : public LogicalOperator {

public:
    LogicalResultCollector(expression_vector expressionsToCollect, unique_ptr<Schema> schema,
        shared_ptr<LogicalOperator> child)
        : LogicalOperator{move(child)},
          expressionsToCollect{move(expressionsToCollect)}, schema{move(schema)} {}

    inline LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_RESULT_COLLECTOR;
    }

    string getExpressionsForPrinting() const override;

    inline expression_vector getExpressionsToCollect() { return expressionsToCollect; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalResultCollector>(
            expressionsToCollect, schema->copy(), getChild(0)->copy());
    }

    inline Schema* getSchema() { return schema.get(); }

private:
    expression_vector expressionsToCollect;
    unique_ptr<Schema> schema;
};

} // namespace planner
} // namespace graphflow
