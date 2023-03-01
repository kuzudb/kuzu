#pragma once

#include "planner/logical_plan/logical_plan.h"

namespace kuzu {
namespace optimizer {

// Due to the nature of graph pattern, a (node)-[rel]-(node) is always interpreted as two joins.
// However, in many cases, a single join is sufficient.
// E.g. MATCH (a)-[e]->(b) RETURN e.date
// Our planner will generate a plan where the HJ is redundant.
//      HJ
//     /  \
//   E(e) S(b)
//    |
//   S(a)
// This optimizer prunes such redundant joins.
class RemoveUnnecessaryJoinOptimizer {
public:
    static void rewrite(planner::LogicalPlan* plan);

private:
    static std::shared_ptr<planner::LogicalOperator> visitOperator(
        std::shared_ptr<planner::LogicalOperator> op);
    static std::shared_ptr<planner::LogicalOperator> visitHashJoin(
        std::shared_ptr<planner::LogicalOperator> op);
};

} // namespace optimizer
} // namespace kuzu
