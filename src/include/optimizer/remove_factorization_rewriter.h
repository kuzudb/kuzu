#pragma once

#include "logical_operator_visitor.h"
#include "planner/logical_plan/logical_plan.h"

namespace kuzu {
namespace optimizer {

class RemoveFactorizationRewriter : public LogicalOperatorVisitor {
public:
    void rewrite(planner::LogicalPlan* plan);

private:
    std::shared_ptr<planner::LogicalOperator> visitOperator(
        std::shared_ptr<planner::LogicalOperator> op);

    std::shared_ptr<planner::LogicalOperator> visitFlattenReplace(
        std::shared_ptr<planner::LogicalOperator> op) override;

    class Verifier : public LogicalOperatorVisitor {
    public:
        Verifier() : containsFlatten_{false} {}

        inline bool containsFlatten() const { return containsFlatten_; }

        void visit(planner::LogicalOperator* op);

    private:
        void visitFlatten(planner::LogicalOperator* op) override;

    private:
        bool containsFlatten_;
    };
};

} // namespace optimizer
} // namespace kuzu
