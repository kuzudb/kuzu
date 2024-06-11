#pragma once

#include "planner/operator/extend/base_logical_extend.h"
#include "planner/operator/extend/recursive_join_type.h"
#include "planner/operator/sip/side_way_info_passing.h"

namespace kuzu {
namespace planner {

class LogicalRecursiveExtend : public BaseLogicalExtend {
public:
    LogicalRecursiveExtend(std::shared_ptr<binder::NodeExpression> boundNode,
        std::shared_ptr<binder::NodeExpression> nbrNode, std::shared_ptr<binder::RelExpression> rel,
        common::ExtendDirection direction, RecursiveJoinType joinType,
        std::shared_ptr<LogicalOperator> child, std::shared_ptr<LogicalOperator> recursiveChild)
        : BaseLogicalExtend{LogicalOperatorType::RECURSIVE_EXTEND, std::move(boundNode),
              std::move(nbrNode), std::move(rel), direction, std::move(child)},
          joinType{joinType}, recursiveChild{std::move(recursiveChild)} {}

    f_group_pos_set getGroupsPosToFlatten() override;

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    void setJoinType(RecursiveJoinType joinType_) { joinType = joinType_; }
    RecursiveJoinType getJoinType() const { return joinType; }
    std::shared_ptr<LogicalOperator> getRecursiveChild() const { return recursiveChild; }

    std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalRecursiveExtend>(boundNode, nbrNode, rel, direction,
            joinType, children[0]->copy(), recursiveChild->copy());
    }

private:
    RecursiveJoinType joinType;
    std::shared_ptr<LogicalOperator> recursiveChild;
};

class LogicalPathPropertyProbe : public LogicalOperator {
    static constexpr LogicalOperatorType type_ = LogicalOperatorType::PATH_PROPERTY_PROBE;

public:
    LogicalPathPropertyProbe(std::shared_ptr<binder::RelExpression> recursiveRel,
        std::shared_ptr<LogicalOperator> probeChild, std::shared_ptr<LogicalOperator> nodeChild,
        std::shared_ptr<LogicalOperator> relChild, RecursiveJoinType joinType)
        : LogicalOperator{type_, std::move(probeChild)}, recursiveRel{std::move(recursiveRel)},
          nodeChild{std::move(nodeChild)}, relChild{std::move(relChild)}, joinType{joinType} {}

    void computeFactorizedSchema() final;
    void computeFlatSchema() final;

    std::string getExpressionsForPrinting() const override { return recursiveRel->toString(); }

    std::shared_ptr<binder::RelExpression> getRel() const { return recursiveRel; }

    void setJoinType(RecursiveJoinType joinType_) { joinType = joinType_; }
    RecursiveJoinType getJoinType() const { return joinType; }

    std::shared_ptr<LogicalOperator> getNodeChild() const { return nodeChild; }
    std::shared_ptr<LogicalOperator> getRelChild() const { return relChild; }

    SIPInfo& getSIPInfoUnsafe() { return sipInfo; }
    SIPInfo getSIPInfo() const { return sipInfo; }

    std::unique_ptr<LogicalOperator> copy() override;

private:
    std::shared_ptr<binder::RelExpression> recursiveRel;
    std::shared_ptr<LogicalOperator> nodeChild;
    std::shared_ptr<LogicalOperator> relChild;
    RecursiveJoinType joinType;
    SIPInfo sipInfo;
};

} // namespace planner
} // namespace kuzu
