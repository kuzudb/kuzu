#pragma once

#include "common/enums/accumulate_type.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalAccumulate final : public LogicalOperator {
public:
    LogicalAccumulate(common::AccumulateType accumulateType, binder::expression_vector flatExprs,
        std::shared_ptr<binder::Expression> offset, std::shared_ptr<binder::Expression> mark,
        std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::ACCUMULATE, std::move(child)},
          accumulateType{accumulateType}, flatExprs{std::move(flatExprs)},
          offset{std::move(offset)}, mark{std::move(mark)} {}

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    f_group_pos_set getGroupPositionsToFlatten() const;

    std::string getExpressionsForPrinting() const override { return {}; }

    common::AccumulateType getAccumulateType() const { return accumulateType; }
    binder::expression_vector getPayloads() const {
        return children[0]->getSchema()->getExpressionsInScope();
    }
    std::shared_ptr<binder::Expression> getOffset() const { return offset; }
    bool hasMark() const { return mark != nullptr; }
    std::shared_ptr<binder::Expression> getMark() const { return mark; }

    std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalAccumulate>(accumulateType, flatExprs, offset, mark,
            children[0]->copy());
    }

private:
    common::AccumulateType accumulateType;
    binder::expression_vector flatExprs;
    // Accumulate may be used as a source operator for COPY pipeline. In such case, row offset needs
    // to be provided in order to generate internal ID.
    std::shared_ptr<binder::Expression> offset;
    // Accumulate may be used for optional match, e.g. OPTIONAL MATCH (a). In such case, we use
    // mark to determine if at least one pattern is found.
    std::shared_ptr<binder::Expression> mark;
};

} // namespace planner
} // namespace kuzu
