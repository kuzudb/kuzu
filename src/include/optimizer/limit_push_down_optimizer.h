#pragma once

#include "planner/operator/logical_limit.h"
#include "planner/operator/logical_plan.h"
#include "planner/operator/sip/logical_semi_masker.h"

namespace kuzu {
namespace main {
class ClientContext;
}
namespace optimizer {

class LimitPushDownOptimizer {
public:
    explicit LimitPushDownOptimizer(main::ClientContext* context)
        : limitOperator{nullptr}, context{context} {}

    void rewrite(planner::LogicalPlan* plan);

private:
    std::shared_ptr<planner::LogicalOperator> visitOperator(
        std::shared_ptr<planner::LogicalOperator> op);

    std::shared_ptr<planner::LogicalOperator> finishPushDown(
        std::shared_ptr<planner::LogicalOperator> op);

    static planner::LogicalSemiMasker* findLogicalSemiMasker(
        const std::shared_ptr<planner::LogicalOperator>& op) {
        if (op->getOperatorType() == planner::LogicalOperatorType::SEMI_MASKER) {
            return &op->cast<planner::LogicalSemiMasker>();
        }
        if (op->getNumChildren() != 1) {
            return nullptr;
        }
        return findLogicalSemiMasker(op->getChild(0));
    }

private:
    planner::LogicalLimit* limitOperator;
    main::ClientContext* context;
};

} // namespace optimizer
} // namespace kuzu
