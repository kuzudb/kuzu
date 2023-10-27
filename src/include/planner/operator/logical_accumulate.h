#pragma once

#include "common/enums/join_type.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalAccumulate : public LogicalOperator {
public:
    LogicalAccumulate(common::AccumulateType accumulateType,
        binder::expression_vector expressionsToFlatten, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::ACCUMULATE, std::move(child)},
          accumulateType{accumulateType}, expressionsToFlatten{std::move(expressionsToFlatten)} {}

    void computeFactorizedSchema() final;
    void computeFlatSchema() final;

    f_group_pos_set getGroupPositionsToFlatten() const;

    inline std::string getExpressionsForPrinting() const final { return std::string{}; }

    inline common::AccumulateType getAccumulateType() const { return accumulateType; }
    inline binder::expression_vector getExpressionsToAccumulate() const {
        return children[0]->getSchema()->getExpressionsInScope();
    }

    inline std::unique_ptr<LogicalOperator> copy() final {
        return make_unique<LogicalAccumulate>(
            accumulateType, expressionsToFlatten, children[0]->copy());
    }

private:
    common::AccumulateType accumulateType;
    binder::expression_vector expressionsToFlatten;
};

} // namespace planner
} // namespace kuzu
