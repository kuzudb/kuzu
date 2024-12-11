#pragma once

#include "optimizer/logical_operator_visitor.h"
#include "planner/operator/logical_plan.h"
namespace kuzu {
namespace optimizer {
class SchemaPopulator : public LogicalOperatorVisitor {
public:
    void rewrite(planner::LogicalPlan* plan);
};
} // namespace optimizer
} // namespace kuzu
