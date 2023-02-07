#pragma once

#include "base_logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalAccumulate : public LogicalOperator {
public:
    LogicalAccumulate(binder::expression_vector expressions, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::ACCUMULATE, std::move(child)}, expressions{std::move(
                                                                                  expressions)} {}

    void computeSchema() override;

    inline std::string getExpressionsForPrinting() const override {
        return binder::ExpressionUtil::toString(expressions);
    }

    inline binder::expression_vector getExpressions() const { return expressions; }
    inline Schema* getSchemaBeforeSink() const { return children[0]->getSchema(); }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalAccumulate>(expressions, children[0]->copy());
    }

private:
    binder::expression_vector expressions;
};

} // namespace planner
} // namespace kuzu
