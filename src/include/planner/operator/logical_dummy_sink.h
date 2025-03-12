#pragma once

#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalDummySink final : public LogicalOperator {
    static constexpr LogicalOperatorType type_ = LogicalOperatorType::DUMMY_SINK;

public:
    explicit LogicalDummySink(std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{type_, {std::move(child)}} {}
    explicit LogicalDummySink(logical_op_vector_t children)
        : LogicalOperator{type_, {std::move(children)}} {}

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    std::string getExpressionsForPrinting() const override { return ""; }
    std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalDummySink>(children);
    }
};

} // namespace planner
} // namespace kuzu
