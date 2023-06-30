#pragma once

#include "base_logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalExplain : public LogicalOperator {
public:
    LogicalExplain(std::shared_ptr<LogicalOperator> child,
        std::shared_ptr<binder::Expression> outputExpression)
        : LogicalOperator{LogicalOperatorType::EXPLAIN, child}, outputExpression{
                                                                    std::move(outputExpression)} {}

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline std::shared_ptr<binder::Expression> getOutputExpression() const {
        return outputExpression;
    }

    inline std::string getExpressionsForPrinting() const override { return "Explain"; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalExplain>(children[0], outputExpression);
    }

protected:
    std::shared_ptr<binder::Expression> outputExpression;
};

} // namespace planner
} // namespace kuzu
