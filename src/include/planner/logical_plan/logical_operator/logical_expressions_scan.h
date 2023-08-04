#pragma once

#include "base_logical_operator.h"
#include "binder/expression/expression.h"

namespace kuzu {
namespace planner {

// TODO(Xiyang): change to DummyScan once we rewrite our index scan.
class LogicalExpressionScan : public LogicalOperator {
public:
    explicit LogicalExpressionScan(std::shared_ptr<binder::Expression> expression)
        : LogicalOperator{LogicalOperatorType::EXPRESSION_SCAN}, expression{std::move(expression)} {
    }

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline std::string getExpressionsForPrinting() const override { return std::string(); }

    inline std::shared_ptr<binder::Expression> getExpression() const { return expression; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalExpressionScan>(expression);
    }

private:
    std::shared_ptr<binder::Expression> expression;
};

} // namespace planner
} // namespace kuzu
