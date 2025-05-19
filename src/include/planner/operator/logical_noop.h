#pragma once

#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalNoop : public LogicalOperator {
    static constexpr LogicalOperatorType type_ = LogicalOperatorType::NOOP;

public:
    explicit LogicalNoop(std::vector<std::shared_ptr<LogicalOperator>> children)
        : LogicalOperator{type_, {std::move(children)}} {}

    void computeFactorizedSchema() override { createEmptySchema(); }
    void computeFlatSchema() override { createEmptySchema(); }

    std::string getExpressionsForPrinting() const override { return ""; }

    std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalNoop>(copyVector(children));
    }
};

} // namespace planner
} // namespace kuzu
