#pragma once

#include "base_logical_extend.h"
#include "recursive_join_type.h"

namespace kuzu {
namespace planner {

class LogicalRecursiveExtend : public BaseLogicalExtend {
public:
    LogicalRecursiveExtend(std::shared_ptr<binder::NodeExpression> boundNode,
        std::shared_ptr<binder::NodeExpression> nbrNode, std::shared_ptr<binder::RelExpression> rel,
        common::ExtendDirection direction, std::shared_ptr<LogicalOperator> child)
        : LogicalRecursiveExtend{std::move(boundNode), std::move(nbrNode), std::move(rel),
              direction, RecursiveJoinType::TRACK_PATH, std::move(child)} {}
    LogicalRecursiveExtend(std::shared_ptr<binder::NodeExpression> boundNode,
        std::shared_ptr<binder::NodeExpression> nbrNode, std::shared_ptr<binder::RelExpression> rel,
        common::ExtendDirection direction, RecursiveJoinType joinType,
        std::shared_ptr<LogicalOperator> child)
        : BaseLogicalExtend{LogicalOperatorType::RECURSIVE_EXTEND, std::move(boundNode),
              std::move(nbrNode), std::move(rel), direction, std::move(child)},
          joinType{joinType} {}

    f_group_pos_set getGroupsPosToFlatten() override;

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline void setJoinType(RecursiveJoinType joinType_) { joinType = joinType_; }
    inline RecursiveJoinType getJoinType() const { return joinType; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalRecursiveExtend>(
            boundNode, nbrNode, rel, direction, joinType, children[0]->copy());
    }

private:
    RecursiveJoinType joinType;
};

} // namespace planner
} // namespace kuzu
