#pragma once

#include "base_logical_operator.h"
#include "logical_extend.h"

namespace graphflow {
namespace planner {

class LogicalFixedLengthExtend : public LogicalExtend {

public:
    LogicalFixedLengthExtend(shared_ptr<RelExpression> queryRel, Direction direction,
        bool isColumnExtend, shared_ptr<LogicalOperator> child)
        : LogicalExtend{move(queryRel), direction, move(child)}, isColumnExtend{isColumnExtend} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_FIXED_LENGTH_EXTEND;
    }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalFixedLengthExtend>(
            queryRel, direction, isColumnExtend, children[0]->copy());
    }

    inline bool getIsColumnExtend() const { return isColumnExtend; }

public:
    bool isColumnExtend;
};

} // namespace planner
} // namespace graphflow
