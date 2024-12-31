#pragma once

#include "logical_operator_visitor.h"
#include "planner/operator/logical_plan.h"
#include "main/client_context.h"

namespace kuzu {
namespace optimizer {

class ReorderExtendDirection : public LogicalOperatorVisitor {
public:
    explicit ReorderExtendDirection(main::ClientContext* context): context(context) {}

    void rewrite(planner::LogicalPlan* plan);

private:
    std::shared_ptr<planner::LogicalOperator> visitOperator(
            const std::shared_ptr<planner::LogicalOperator>& op);

    std::shared_ptr<planner::LogicalOperator> visitExtendReplace(
            std::shared_ptr<planner::LogicalOperator> op) override;

private:
    main::ClientContext* context;
};

} // namespace optimizer
} // namespace kuzu

