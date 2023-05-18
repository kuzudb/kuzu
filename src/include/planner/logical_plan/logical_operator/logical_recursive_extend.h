#pragma once

#include "base_logical_extend.h"

namespace kuzu {
namespace planner {

class LogicalRecursiveExtend : public BaseLogicalExtend {
public:
    LogicalRecursiveExtend(std::shared_ptr<binder::NodeExpression> boundNode,
        std::shared_ptr<binder::NodeExpression> nbrNode, std::shared_ptr<binder::RelExpression> rel,
        common::ExtendDirection direction, std::shared_ptr<LogicalOperator> child)
        : LogicalRecursiveExtend{std::move(boundNode), std::move(nbrNode), std::move(rel),
              direction, true /* trackPath */, std::move(child)} {}
    LogicalRecursiveExtend(std::shared_ptr<binder::NodeExpression> boundNode,
        std::shared_ptr<binder::NodeExpression> nbrNode, std::shared_ptr<binder::RelExpression> rel,
        common::ExtendDirection direction, bool trackPath_, std::shared_ptr<LogicalOperator> child)
        : BaseLogicalExtend{LogicalOperatorType::RECURSIVE_EXTEND, std::move(boundNode),
              std::move(nbrNode), std::move(rel), direction, std::move(child)},
          trackPath_{trackPath_} {}

    f_group_pos_set getGroupsPosToFlatten() override;

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline void disableTrackPath() { trackPath_ = false; }
    inline bool trackPath() { return trackPath_; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalRecursiveExtend>(
            boundNode, nbrNode, rel, direction, trackPath_, children[0]->copy());
    }

private:
    bool trackPath_;
};

} // namespace planner
} // namespace kuzu
