#pragma once

#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace optimizer {

class LogicalOperatorVisitor {
public:
    LogicalOperatorVisitor() = default;
    virtual ~LogicalOperatorVisitor() = default;

protected:
    void visitOperatorSwitch(planner::LogicalOperator* op);
    std::shared_ptr<planner::LogicalOperator> visitOperatorReplaceSwitch(
        std::shared_ptr<planner::LogicalOperator> op);

    virtual void visitFlatten(planner::LogicalOperator* op) {}
    virtual std::shared_ptr<planner::LogicalOperator> visitFlattenReplace(
        std::shared_ptr<planner::LogicalOperator> op) {
        return op;
    }

    virtual void visitExpressionsScan(planner::LogicalOperator* op) {}
    virtual std::shared_ptr<planner::LogicalOperator> visitExpressionsScanReplace(
        std::shared_ptr<planner::LogicalOperator> op) {
        return op;
    }

    virtual void visitScanInternalID(planner::LogicalOperator* op) {}
    virtual std::shared_ptr<planner::LogicalOperator> visitScanInternalIDReplace(
        std::shared_ptr<planner::LogicalOperator> op) {
        return op;
    }

    virtual void visitIndexScanNode(planner::LogicalOperator* op) {}
    virtual std::shared_ptr<planner::LogicalOperator> visitIndexScanNodeReplace(
        std::shared_ptr<planner::LogicalOperator> op) {
        return op;
    }

    virtual void visitExtend(planner::LogicalOperator* op) {}
    virtual std::shared_ptr<planner::LogicalOperator> visitExtendReplace(
        std::shared_ptr<planner::LogicalOperator> op) {
        return op;
    }

    virtual void visitRecursiveExtend(planner::LogicalOperator* op) {}
    virtual std::shared_ptr<planner::LogicalOperator> visitRecursiveExtendReplace(
        std::shared_ptr<planner::LogicalOperator> op) {
        return op;
    }

    virtual void visitPathPropertyProbe(planner::LogicalOperator* op) {}
    virtual std::shared_ptr<planner::LogicalOperator> visitPathPropertyProbeReplace(
        std::shared_ptr<planner::LogicalOperator> op) {
        return op;
    }

    virtual void visitHashJoin(planner::LogicalOperator* op) {}
    virtual std::shared_ptr<planner::LogicalOperator> visitHashJoinReplace(
        std::shared_ptr<planner::LogicalOperator> op) {
        return op;
    }

    virtual void visitIntersect(planner::LogicalOperator* op) {}
    virtual std::shared_ptr<planner::LogicalOperator> visitIntersectReplace(
        std::shared_ptr<planner::LogicalOperator> op) {
        return op;
    }

    virtual void visitProjection(planner::LogicalOperator* op) {}
    virtual std::shared_ptr<planner::LogicalOperator> visitProjectionReplace(
        std::shared_ptr<planner::LogicalOperator> op) {
        return op;
    }

    virtual void visitAggregate(planner::LogicalOperator* op) {}
    virtual std::shared_ptr<planner::LogicalOperator> visitAggregateReplace(
        std::shared_ptr<planner::LogicalOperator> op) {
        return op;
    }

    virtual void visitOrderBy(planner::LogicalOperator* op) {}
    virtual std::shared_ptr<planner::LogicalOperator> visitOrderByReplace(
        std::shared_ptr<planner::LogicalOperator> op) {
        return op;
    }

    virtual void visitLimit(planner::LogicalOperator* op) {}
    virtual std::shared_ptr<planner::LogicalOperator> visitLimitReplace(
        std::shared_ptr<planner::LogicalOperator> op) {
        return op;
    }

    virtual void visitAccumulate(planner::LogicalOperator* op) {}
    virtual std::shared_ptr<planner::LogicalOperator> visitAccumulateReplace(
        std::shared_ptr<planner::LogicalOperator> op) {
        return op;
    }

    virtual void visitDistinct(planner::LogicalOperator* op) {}
    virtual std::shared_ptr<planner::LogicalOperator> visitDistinctReplace(
        std::shared_ptr<planner::LogicalOperator> op) {
        return op;
    }

    virtual void visitUnwind(planner::LogicalOperator* op) {}
    virtual std::shared_ptr<planner::LogicalOperator> visitUnwindReplace(
        std::shared_ptr<planner::LogicalOperator> op) {
        return op;
    }

    virtual void visitUnion(planner::LogicalOperator* op) {}
    virtual std::shared_ptr<planner::LogicalOperator> visitUnionReplace(
        std::shared_ptr<planner::LogicalOperator> op) {
        return op;
    }

    virtual void visitFilter(planner::LogicalOperator* op) {}
    virtual std::shared_ptr<planner::LogicalOperator> visitFilterReplace(
        std::shared_ptr<planner::LogicalOperator> op) {
        return op;
    }

    virtual void visitSetNodeProperty(planner::LogicalOperator* op) {}
    virtual std::shared_ptr<planner::LogicalOperator> visitSetNodePropertyReplace(
        std::shared_ptr<planner::LogicalOperator> op) {
        return op;
    }

    virtual void visitSetRelProperty(planner::LogicalOperator* op) {}
    virtual std::shared_ptr<planner::LogicalOperator> visitSetRelPropertyReplace(
        std::shared_ptr<planner::LogicalOperator> op) {
        return op;
    }

    virtual void visitDeleteNode(planner::LogicalOperator* op) {}
    virtual std::shared_ptr<planner::LogicalOperator> visitDeleteNodeReplace(
        std::shared_ptr<planner::LogicalOperator> op) {
        return op;
    }

    virtual void visitDeleteRel(planner::LogicalOperator* op) {}
    virtual std::shared_ptr<planner::LogicalOperator> visitDeleteRelReplace(
        std::shared_ptr<planner::LogicalOperator> op) {
        return op;
    }

    virtual void visitInsertNode(planner::LogicalOperator* op) {}
    virtual std::shared_ptr<planner::LogicalOperator> visitInsertNodeReplace(
        std::shared_ptr<planner::LogicalOperator> op) {
        return op;
    }

    virtual void visitInsertRel(planner::LogicalOperator* op) {}
    virtual std::shared_ptr<planner::LogicalOperator> visitInsertRelReplace(
        std::shared_ptr<planner::LogicalOperator> op) {
        return op;
    }

    virtual void visitMerge(planner::LogicalOperator* op) {}
    virtual std::shared_ptr<planner::LogicalOperator> visitMergeReplace(
        std::shared_ptr<planner::LogicalOperator> op) {
        return op;
    }

    virtual void visitCopyTo(planner::LogicalOperator* op) {}
    virtual std::shared_ptr<planner::LogicalOperator> visitCopyToReplace(
        std::shared_ptr<planner::LogicalOperator> op) {
        return op;
    }
};

} // namespace optimizer
} // namespace kuzu
