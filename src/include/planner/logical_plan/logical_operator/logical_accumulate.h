#pragma once

#include "base_logical_operator.h"
#include "schema.h"

namespace kuzu {
namespace planner {

class LogicalAccumulate : public LogicalOperator {
public:
    LogicalAccumulate(expression_vector expressions, unique_ptr<Schema> schemaBeforeSink,
        shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::ACCUMULATE, std::move(child)},
          expressions{std::move(expressions)}, schemaBeforeSink{std::move(schemaBeforeSink)} {}

    string getExpressionsForPrinting() const override {
        return ExpressionUtil::toString(expressions);
    }

    inline expression_vector getExpressions() const { return expressions; }
    inline Schema* getSchemaBeforeSink() const { return schemaBeforeSink.get(); }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalAccumulate>(
            expressions, schemaBeforeSink->copy(), children[0]->copy());
    }

private:
    expression_vector expressions;
    unique_ptr<Schema> schemaBeforeSink;
};

} // namespace planner
} // namespace kuzu
