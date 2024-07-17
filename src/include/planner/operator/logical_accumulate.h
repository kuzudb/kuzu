#pragma once

#include "common/enums/accumulate_type.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

struct LogicalAccumulateInfo {
    common::AccumulateType accumulateType;
    // Accumulate may be used for optional match, e.g. OPTIONAL MATCH (a). In such case, we use
    // mark to determine if at least one pattern is found.
    std::shared_ptr<binder::Expression> mark;

    LogicalAccumulateInfo(common::AccumulateType type, std::shared_ptr<binder::Expression> mark)
        : accumulateType{type}, mark{std::move(mark)} {}
};

class LogicalAccumulate final : public LogicalOperator {
    static constexpr LogicalOperatorType type_ = LogicalOperatorType::ACCUMULATE;

public:
    LogicalAccumulate(LogicalAccumulateInfo info, binder::expression_vector flatExprs,
        std::shared_ptr<binder::Expression> offset,
        std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{type_, std::move(child)},
          info{std::move(info)}, flatExprs{std::move(flatExprs)},
          offset{std::move(offset)} {}

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    f_group_pos_set getGroupPositionsToFlatten() const;

    std::string getExpressionsForPrinting() const override { return {}; }

    common::AccumulateType getAccumulateType() const { return info.accumulateType; }
    binder::expression_vector getPayloads() const {
        return children[0]->getSchema()->getExpressionsInScope();
    }
    std::shared_ptr<binder::Expression> getOffset() const { return offset; }
    bool hasMark() const { return info.mark != nullptr; }
    std::shared_ptr<binder::Expression> getMark() const { return info.mark; }

    std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalAccumulate>(info, flatExprs, offset,
            children[0]->copy());
    }

private:
    LogicalAccumulateInfo info;
    binder::expression_vector flatExprs;
    // Accumulate may be used as a source operator for COPY pipeline. In such case, row offset needs
    // to be provided in order to generate internal ID.
    std::shared_ptr<binder::Expression> offset;
};

} // namespace planner
} // namespace kuzu
