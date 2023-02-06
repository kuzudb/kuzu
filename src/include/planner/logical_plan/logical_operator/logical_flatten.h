#pragma once

#include "base_logical_operator.h"
#include "binder/expression/expression.h"

namespace kuzu {
namespace planner {

class LogicalFlatten : public LogicalOperator {
public:
    LogicalFlatten(
        std::shared_ptr<binder::Expression> expression, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::FLATTEN, std::move(child)}, expression{std::move(
                                                                               expression)} {}

    void computeSchema() override;

    inline std::string getExpressionsForPrinting() const override {
        return expression->getUniqueName();
    }

    inline std::shared_ptr<binder::Expression> getExpression() const { return expression; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalFlatten>(expression, children[0]->copy());
    }

private:
    std::shared_ptr<binder::Expression> expression;
};

} // namespace planner
} // namespace kuzu
