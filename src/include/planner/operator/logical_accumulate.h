#pragma once

#include "common/enums/join_type.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalAccumulate final : public LogicalOperator {
public:
    LogicalAccumulate(common::AccumulateType accumulateType, binder::expression_vector flatExprs,
        std::shared_ptr<binder::Expression> offset, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::ACCUMULATE, std::move(child)},
          accumulateType{accumulateType}, flatExprs{std::move(flatExprs)},
          offset{std::move(offset)} {}

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    f_group_pos_set getGroupPositionsToFlatten() const;

    std::string getExpressionsForPrinting() const override { return {}; }

    common::AccumulateType getAccumulateType() const { return accumulateType; }
    binder::expression_vector getPayloads() const {
        return children[0]->getSchema()->getExpressionsInScope();
    }
    std::shared_ptr<binder::Expression> getOffset() const { return offset; }

    std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalAccumulate>(accumulateType, flatExprs, offset,
            children[0]->copy());
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
