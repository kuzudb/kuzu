#pragma once

#include "common/enums/join_type.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalAccumulate : public LogicalOperator {
public:
    LogicalAccumulate(common::AccumulateType accumulateType, binder::expression_vector flatExprs,
        std::shared_ptr<binder::Expression> offset, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::ACCUMULATE, std::move(child)},
          accumulateType{accumulateType}, flatExprs{std::move(flatExprs)}, offset{
                                                                               std::move(offset)} {}

    void computeFactorizedSchema() final;
    void computeFlatSchema() final;

    f_group_pos_set getGroupPositionsToFlatten() const;

    inline std::string getExpressionsForPrinting() const final { return std::string{}; }

    inline common::AccumulateType getAccumulateType() const { return accumulateType; }
    inline binder::expression_vector getExpressionsToAccumulate() const {
        return children[0]->getSchema()->getExpressionsInScope();
    }
    inline std::shared_ptr<binder::Expression> getOffset() const { return offset; }

    inline std::unique_ptr<LogicalOperator> copy() final {
        return make_unique<LogicalAccumulate>(
            accumulateType, flatExprs, offset, children[0]->copy());
    }

private:
    common::AccumulateType accumulateType;
    binder::expression_vector flatExprs;
    // Accumulate may be used as a source operator for COPY pipeline. In such case, row offset needs
    // to be provided in order to generate internal ID.
    std::shared_ptr<binder::Expression> offset;
};

} // namespace planner
} // namespace kuzu
