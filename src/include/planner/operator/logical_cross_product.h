#pragma once

#include "common/enums/accumulate_type.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalCrossProduct : public LogicalOperator {
    static constexpr LogicalOperatorType type_ = LogicalOperatorType::CROSS_PRODUCT;

public:
    LogicalCrossProduct(common::AccumulateType accumulateType,
        std::shared_ptr<binder::Expression> mark, std::shared_ptr<LogicalOperator> probeChild,
        std::shared_ptr<LogicalOperator> buildChild)
        : LogicalOperator{type_, std::move(probeChild), std::move(buildChild)},
          accumulateType{accumulateType}, mark{std::move(mark)} {}

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    std::string getExpressionsForPrinting() const override { return std::string(); }

    common::AccumulateType getAccumulateType() const { return accumulateType; }
    bool hasMark() const { return mark != nullptr; }
    std::shared_ptr<binder::Expression> getMark() const { return mark; }

    std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCrossProduct>(accumulateType, mark, children[0]->copy(),
            children[1]->copy());
    }

private:
    common::AccumulateType accumulateType;
    std::shared_ptr<binder::Expression> mark;
};

} // namespace planner
} // namespace kuzu
