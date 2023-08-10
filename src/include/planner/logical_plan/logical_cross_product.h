#pragma once

#include "common/join_type.h"
#include "logical_operator.h"
#include "planner/logical_plan/factorization/sink_util.h"

namespace kuzu {
namespace planner {

class LogicalCrossProduct : public LogicalOperator {
public:
    LogicalCrossProduct(common::AccumulateType accumulateType,
        std::shared_ptr<LogicalOperator> probeChild, std::shared_ptr<LogicalOperator> buildChild)
        : LogicalOperator{LogicalOperatorType::CROSS_PRODUCT, std::move(probeChild),
              std::move(buildChild)},
          accumulateType{accumulateType} {}

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline std::string getExpressionsForPrinting() const override { return std::string(); }

    inline common::AccumulateType getAccumulateType() const { return accumulateType; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCrossProduct>(
            accumulateType, children[0]->copy(), children[1]->copy());
    }

private:
    common::AccumulateType accumulateType;
};

} // namespace planner
} // namespace kuzu
