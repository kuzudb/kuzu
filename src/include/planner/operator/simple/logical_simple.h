#pragma once

#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalSimple : public LogicalOperator {
public:
    LogicalSimple(LogicalOperatorType operatorType,
        std::shared_ptr<binder::Expression> outputExpression)
        : LogicalOperator{operatorType}, outputExpression{std::move(outputExpression)} {}
    LogicalSimple(LogicalOperatorType operatorType,
        const std::vector<std::shared_ptr<LogicalOperator>>& plans,
        std::shared_ptr<binder::Expression> outputExpression)
        : LogicalOperator{operatorType, plans}, outputExpression{std::move(outputExpression)} {}

    void computeFactorizedSchema() override;

    void computeFlatSchema() override;

    std::shared_ptr<binder::Expression> getOutputExpression() const { return outputExpression; }

protected:
    std::shared_ptr<binder::Expression> outputExpression;
};

} // namespace planner
} // namespace kuzu
