#pragma once

#include "base_logical_extend.h"

namespace kuzu {
namespace planner {

// TODO(Xiyang): we should have a single LogicalRecursiveExtend once we migrate VariableLengthExtend
// to use the same infrastructure as shortest path.
class LogicalRecursiveExtend : public BaseLogicalExtend {
public:
    LogicalRecursiveExtend(std::shared_ptr<binder::NodeExpression> boundNode,
        std::shared_ptr<binder::NodeExpression> nbrNode, std::shared_ptr<binder::RelExpression> rel,
        common::RelDirection direction, std::shared_ptr<LogicalOperator> child)
        : BaseLogicalExtend{LogicalOperatorType::RECURSIVE_EXTEND, std::move(boundNode),
              std::move(nbrNode), std::move(rel), direction, std::move(child)} {}

    f_group_pos_set getGroupsPosToFlatten() override;

    void computeFlatSchema() override;
};

class LogicalVariableLengthExtend : public LogicalRecursiveExtend {
public:
    LogicalVariableLengthExtend(std::shared_ptr<binder::NodeExpression> boundNode,
        std::shared_ptr<binder::NodeExpression> nbrNode, std::shared_ptr<binder::RelExpression> rel,
        common::RelDirection direction, bool hasAtMostOneNbr,
        std::shared_ptr<LogicalOperator> child)
        : LogicalRecursiveExtend{std::move(boundNode), std::move(nbrNode), std::move(rel),
              direction, std::move(child)},
          hasAtMostOneNbr{hasAtMostOneNbr} {}

    void computeFactorizedSchema() override;

    inline std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalVariableLengthExtend>(
            boundNode, nbrNode, rel, direction, hasAtMostOneNbr, children[0]->copy());
    }

private:
    bool hasAtMostOneNbr;
};

class LogicalShortestPathExtend : public LogicalRecursiveExtend {
public:
    LogicalShortestPathExtend(std::shared_ptr<binder::NodeExpression> boundNode,
        std::shared_ptr<binder::NodeExpression> nbrNode, std::shared_ptr<binder::RelExpression> rel,
        common::RelDirection direction, std::shared_ptr<LogicalOperator> child)
        : LogicalRecursiveExtend{std::move(boundNode), std::move(nbrNode), std::move(rel),
              direction, std::move(child)} {}

    void computeFactorizedSchema() override;

    inline std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalShortestPathExtend>(
            boundNode, nbrNode, rel, direction, children[0]->copy());
    }
};

} // namespace planner
} // namespace kuzu
