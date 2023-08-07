#pragma once

#include "common/join_type.h"
#include "logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalAccumulate : public LogicalOperator {
public:
    LogicalAccumulate(common::AccumulateType accumulateType, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::ACCUMULATE, std::move(child)}, accumulateType{
                                                                                  accumulateType} {}

    void computeFactorizedSchema() final;
    void computeFlatSchema() final;

    inline std::string getExpressionsForPrinting() const final { return std::string{}; }

    inline common::AccumulateType getAccumulateType() const { return accumulateType; }
    inline binder::expression_vector getExpressions() const {
        return children[0]->getSchema()->getExpressionsInScope();
    }

    inline std::unique_ptr<LogicalOperator> copy() final {
        return make_unique<LogicalAccumulate>(accumulateType, children[0]->copy());
    }

private:
    common::AccumulateType accumulateType;
};

} // namespace planner
} // namespace kuzu
