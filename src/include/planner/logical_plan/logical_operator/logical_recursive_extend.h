#pragma once

#include "base_logical_extend.h"
#include "recursive_join_type.h"
#include "side_way_info_passing.h"

namespace kuzu {
namespace planner {

class LogicalRecursiveExtend : public BaseLogicalExtend {
public:
    LogicalRecursiveExtend(std::shared_ptr<binder::NodeExpression> boundNode,
        std::shared_ptr<binder::NodeExpression> nbrNode, std::shared_ptr<binder::RelExpression> rel,
        ExtendDirection direction, RecursiveJoinType joinType,
        std::shared_ptr<LogicalOperator> child, std::shared_ptr<LogicalOperator> recursiveChild)
        : BaseLogicalExtend{LogicalOperatorType::RECURSIVE_EXTEND, std::move(boundNode),
              std::move(nbrNode), std::move(rel), direction, std::move(child)},
          joinType{joinType}, recursiveChild{std::move(recursiveChild)} {}

    f_group_pos_set getGroupsPosToFlatten() override;

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline void setJoinType(RecursiveJoinType joinType_) { joinType = joinType_; }
    inline RecursiveJoinType getJoinType() const { return joinType; }
    inline std::shared_ptr<LogicalOperator> getRecursiveChild() const { return recursiveChild; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalRecursiveExtend>(boundNode, nbrNode, rel, direction,
            joinType, children[0]->copy(), recursiveChild->copy());
    }

private:
    RecursiveJoinType joinType;
    std::shared_ptr<LogicalOperator> recursiveChild;
};

class LogicalPathPropertyProbe : public LogicalOperator {
public:
    LogicalPathPropertyProbe(std::shared_ptr<binder::RelExpression> recursiveRel,
        std::shared_ptr<LogicalOperator> probeChild, std::shared_ptr<LogicalOperator> nodeChild,
        std::shared_ptr<LogicalOperator> relChild)
        : LogicalOperator{LogicalOperatorType::PATH_PROPERTY_PROBE,
              std::vector<std::shared_ptr<LogicalOperator>>{
                  std::move(probeChild), std::move(nodeChild), std::move(relChild)}},
          recursiveRel{std::move(recursiveRel)}, sip{SidewaysInfoPassing::NONE} {}

    void computeFactorizedSchema() override { copyChildSchema(0); }
    void computeFlatSchema() override { copyChildSchema(0); }

    std::string getExpressionsForPrinting() const override { return recursiveRel->toString(); }

    inline std::shared_ptr<binder::RelExpression> getRel() const { return recursiveRel; }
    inline void setSIP(SidewaysInfoPassing sip_) { sip = sip_; }
    inline SidewaysInfoPassing getSIP() const { return sip; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalPathPropertyProbe>(
            recursiveRel, children[0]->copy(), children[1]->copy(), children[2]->copy());
    }

private:
    std::shared_ptr<binder::RelExpression> recursiveRel;
    SidewaysInfoPassing sip;
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
