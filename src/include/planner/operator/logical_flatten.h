#pragma once

#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalFlatten : public LogicalOperator {
public:
    LogicalFlatten(f_group_pos groupPos, std::shared_ptr<LogicalOperator> child,
        std::unique_ptr<OPPrintInfo> printInfo)
        : LogicalOperator{LogicalOperatorType::FLATTEN, std::move(child), std::move(printInfo)},
          groupPos{groupPos} {}

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline std::string getExpressionsForPrinting() const override { return std::string{}; }

    inline f_group_pos getGroupPos() const { return groupPos; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalFlatten>(groupPos, children[0]->copy(), printInfo->copy());
    }

private:
    f_group_pos groupPos;
};

} // namespace planner
} // namespace kuzu
