#pragma once

#include "base_logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalAccumulate : public LogicalOperator {
public:
    LogicalAccumulate(expression_vector expressions, shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::ACCUMULATE, std::move(child)}, expressions{std::move(
                                                                                  expressions)} {}

    void computeSchema() override;

    inline string getExpressionsForPrinting() const override {
        return ExpressionUtil::toString(expressions);
    }

    inline expression_vector getExpressions() const { return expressions; }
    inline Schema* getSchemaBeforeSink() const { return children[0]->getSchema(); }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalAccumulate>(expressions, children[0]->copy());
    }

private:
    expression_vector expressions;
};

} // namespace planner
} // namespace kuzu
