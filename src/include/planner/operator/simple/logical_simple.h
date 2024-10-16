#pragma once

#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalSimple : public LogicalOperator {
public:
    LogicalSimple(LogicalOperatorType operatorType,
        std::shared_ptr<binder::Expression> outputExpression,
        std::unique_ptr<OPPrintInfo> printInfo)
        : LogicalOperator{operatorType, std::move(printInfo)},
          outputExpression{std::move(outputExpression)} {}
    LogicalSimple(LogicalOperatorType operatorType,
        std::vector<std::shared_ptr<LogicalOperator>> plans,
        std::shared_ptr<binder::Expression> outputExpression,
        std::unique_ptr<OPPrintInfo> printInfo)
        : LogicalOperator{operatorType, std::move(plans), std::move(printInfo)},
          outputExpression{std::move(outputExpression)} {}

    void computeFactorizedSchema() override;

    void computeFlatSchema() override;

    std::shared_ptr<binder::Expression> getOutputExpression() const { return outputExpression; }

protected:
    std::shared_ptr<binder::Expression> outputExpression;
};

} // namespace planner
} // namespace kuzu
