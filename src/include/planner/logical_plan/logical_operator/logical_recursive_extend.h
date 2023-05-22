#pragma once

#include "base_logical_extend.h"
#include "recursive_join_type.h"

namespace kuzu {
namespace planner {

class LogicalRecursiveExtend : public BaseLogicalExtend {
public:
    LogicalRecursiveExtend(std::shared_ptr<binder::NodeExpression> boundNode,
        std::shared_ptr<binder::NodeExpression> nbrNode, std::shared_ptr<binder::RelExpression> rel,
        ExtendDirection direction, std::shared_ptr<LogicalOperator> child,
        std::shared_ptr<LogicalOperator> recursivePlanRoot)
        : LogicalRecursiveExtend{std::move(boundNode), std::move(nbrNode), std::move(rel),
              direction, RecursiveJoinType::TRACK_PATH, std::move(child),
              std::move(recursivePlanRoot)} {}
    LogicalRecursiveExtend(std::shared_ptr<binder::NodeExpression> boundNode,
        std::shared_ptr<binder::NodeExpression> nbrNode, std::shared_ptr<binder::RelExpression> rel,
        ExtendDirection direction, RecursiveJoinType joinType,
        std::shared_ptr<LogicalOperator> child, std::shared_ptr<LogicalOperator> recursivePlanRoot)
        : BaseLogicalExtend{LogicalOperatorType::RECURSIVE_EXTEND, std::move(boundNode),
              std::move(nbrNode), std::move(rel), direction, std::move(child)},
          joinType{joinType}, recursivePlanRoot{std::move(recursivePlanRoot)} {}

    f_group_pos_set getGroupsPosToFlatten() override;

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline void setJoinType(RecursiveJoinType joinType_) { joinType = joinType_; }
    inline RecursiveJoinType getJoinType() const { return joinType; }
    inline std::shared_ptr<LogicalOperator> getRecursivePlanRoot() const {
        return recursivePlanRoot;
    }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalRecursiveExtend>(boundNode, nbrNode, rel, direction,
            joinType, children[0]->copy(), recursivePlanRoot->copy());
    }

private:
    RecursiveJoinType joinType;
    std::shared_ptr<LogicalOperator> recursivePlanRoot;
};

class LogicalScanFrontier : public LogicalOperator {
public:
    LogicalScanFrontier(std::shared_ptr<binder::NodeExpression> node)
        : LogicalOperator{LogicalOperatorType::SCAN_FRONTIER}, node{std::move(node)} {}

    void computeFactorizedSchema() final;
    void computeFlatSchema() final;

    std::string getExpressionsForPrinting() const override { return std::string(); }

    inline std::shared_ptr<binder::NodeExpression> getNode() const { return node; }

    std::unique_ptr<LogicalOperator> copy() final {
        return std::make_unique<LogicalScanFrontier>(node);
    }

private:
    std::shared_ptr<binder::NodeExpression> node;
};

} // namespace planner
} // namespace kuzu
