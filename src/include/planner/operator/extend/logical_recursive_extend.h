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
        std::shared_ptr<LogicalOperator> relChild, RecursiveJoinType joinType)
        : LogicalOperator{LogicalOperatorType::PATH_PROPERTY_PROBE, std::move(probeChild)},
          recursiveRel{std::move(recursiveRel)}, nodeChild{std::move(nodeChild)},
          relChild{std::move(relChild)}, joinType{joinType}, sip{SidewaysInfoPassing::NONE} {}

    void computeFactorizedSchema() final;
    void computeFlatSchema() final;

    std::string getExpressionsForPrinting() const override { return recursiveRel->toString(); }

    inline std::shared_ptr<binder::RelExpression> getRel() const { return recursiveRel; }

    inline void setJoinType(RecursiveJoinType joinType_) { joinType = joinType_; }
    inline RecursiveJoinType getJoinType() const { return joinType; }

    inline void setSIP(SidewaysInfoPassing sip_) { sip = sip_; }
    inline SidewaysInfoPassing getSIP() const { return sip; }
    inline std::shared_ptr<LogicalOperator> getNodeChild() const { return nodeChild; }
    inline std::shared_ptr<LogicalOperator> getRelChild() const { return relChild; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        auto nodeChildCopy = nodeChild == nullptr ? nullptr : nodeChild->copy();
        auto relChildCopy = relChild == nullptr ? nullptr : relChild->copy();
        return std::make_unique<LogicalPathPropertyProbe>(recursiveRel, children[0]->copy(),
            std::move(nodeChildCopy), std::move(relChildCopy), joinType);
    }

private:
    std::shared_ptr<binder::RelExpression> recursiveRel;
    std::shared_ptr<LogicalOperator> nodeChild;
    std::shared_ptr<LogicalOperator> relChild;
    RecursiveJoinType joinType;
    SidewaysInfoPassing sip;
};

class LogicalScanFrontier : public LogicalOperator {
public:
    LogicalScanFrontier(std::shared_ptr<binder::Expression> nodeID,
        std::shared_ptr<binder::Expression> nodePredicateExecFlag)
        : LogicalOperator{LogicalOperatorType::SCAN_FRONTIER}, nodeID{std::move(nodeID)},
          nodePredicateExecFlag{std::move(nodePredicateExecFlag)} {}

    void computeFactorizedSchema() final;
    void computeFlatSchema() final;

    std::string getExpressionsForPrinting() const override { return std::string(); }

    inline std::shared_ptr<binder::Expression> getNodeID() const { return nodeID; }
    inline std::shared_ptr<binder::Expression> getNodePredicateExecutionFlag() const {
        return nodePredicateExecFlag;
    }

    inline std::unique_ptr<LogicalOperator> copy() final {
        return std::make_unique<LogicalScanFrontier>(nodeID, nodePredicateExecFlag);
    }

private:
    std::shared_ptr<binder::Expression> nodeID;
    std::shared_ptr<binder::Expression> nodePredicateExecFlag;
};

} // namespace planner
} // namespace kuzu
