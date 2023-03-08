#pragma once

#include "base_logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalAccumulate : public LogicalOperator {
public:
    LogicalAccumulate(std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::ACCUMULATE, std::move(child)} {}

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline std::string getExpressionsForPrinting() const override { return std::string{}; }

    inline binder::expression_vector getExpressions() const {
        return children[0]->getSchema()->getExpressionsInScope();
    }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalAccumulate>(children[0]->copy());
    }
};

} // namespace planner
} // namespace kuzu
