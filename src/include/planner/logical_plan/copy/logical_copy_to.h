#pragma once

#include "common/copier_config/copier_config.h"
#include "planner/logical_plan/logical_operator.h"
#include "planner/logical_plan/logical_plan.h"

namespace kuzu {
namespace planner {

class LogicalCopyTo : public LogicalOperator {
public:
    LogicalCopyTo(std::shared_ptr<LogicalOperator> child,
        std::unique_ptr<common::CopyDescription> copyDescription)
        : LogicalOperator{LogicalOperatorType::COPY_TO, std::move(child)},
          copyDescription{std::move(copyDescription)} {}

    f_group_pos_set getGroupsPosToFlatten();

    inline std::string getExpressionsForPrinting() const override { return std::string{}; }

    inline common::CopyDescription* getCopyDescription() const { return copyDescription.get(); }

    void computeFactorizedSchema() override;

    void computeFlatSchema() override;

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalCopyTo>(children[0]->copy(), copyDescription->copy());
    }

private:
    std::shared_ptr<binder::Expression> outputExpression;
    std::unique_ptr<common::CopyDescription> copyDescription;
};

} // namespace planner
} // namespace kuzu
