#pragma once

#include "binder/binder.h"
#include "binder/expression/expression.h"
#include "logical_operator_visitor.h"
#include "planner/operator/logical_plan.h"
namespace kuzu {
namespace main {
class ClientContext;
}
namespace catalog {
class Catalog;
}
namespace binder {}
namespace optimizer {

class PathSemanticRewriter : public LogicalOperatorVisitor {
public:
    explicit PathSemanticRewriter(main::ClientContext* context) : context{context} {}
    void rewrite(planner::LogicalPlan* plan);

private:
    void visitOperator(const std::shared_ptr<planner::LogicalOperator>& op,
        const std::shared_ptr<planner::LogicalOperator>& parent, int index);
    std::shared_ptr<planner::LogicalOperator> visitHashJoinReplace(
        std::shared_ptr<planner::LogicalOperator> op) override;
    std::shared_ptr<planner::LogicalOperator> visitCrossProductReplace(
        std::shared_ptr<planner::LogicalOperator> op);

private:
    bool hasRecursive = false;
    std::shared_ptr<planner::LogicalOperator> topOp = nullptr;
    int replaceIndex = -1;
    main::ClientContext* context;
    binder::expression_vector scanExpression;
    std::shared_ptr<planner::LogicalOperator> appendPathSemanticFilter(
        const std::shared_ptr<planner::LogicalOperator> op);
    std::shared_ptr<binder::Expression> createNode(const std::shared_ptr<binder::Expression>& expr);
    std::shared_ptr<binder::Expression> createRel(const std::shared_ptr<binder::Expression>& expr);
};

} // namespace optimizer
} // namespace kuzu
