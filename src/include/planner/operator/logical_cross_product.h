#pragma once

#include "logical_accumulate.h"

namespace kuzu {
namespace planner {

class LogicalCrossProduct : public LogicalOperator {
    static constexpr LogicalOperatorType type_ = LogicalOperatorType::CROSS_PRODUCT;

public:
    LogicalCrossProduct(LogicalAccumulateInfo info, std::shared_ptr<LogicalOperator> probeChild,
        std::shared_ptr<LogicalOperator> buildChild)
        : LogicalOperator{type_, std::move(probeChild), std::move(buildChild)},
          info{std::move(info)} {}

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    std::string getExpressionsForPrinting() const override { return std::string(); }

    common::AccumulateType getAccumulateType() const { return info.accumulateType; }
    bool hasMark() const { return info.mark != nullptr; }
    std::shared_ptr<binder::Expression> getMark() const { return info.mark; }

    std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCrossProduct>(info, children[0]->copy(), children[1]->copy());
    }

private:
    LogicalAccumulateInfo info;
};

} // namespace planner
} // namespace kuzu
