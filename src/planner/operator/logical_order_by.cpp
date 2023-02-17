#include "planner/logical_plan/logical_operator/logical_order_by.h"

#include "planner/logical_plan/logical_operator/factorization_resolver.h"
#include "planner/logical_plan/logical_operator/sink_util.h"

namespace kuzu {
namespace planner {

void LogicalOrderBy::computeSchema() {
    auto childSchema = children[0]->getSchema();
    schema = std::make_unique<Schema>();
    SinkOperatorUtil::recomputeSchema(*childSchema, expressionsToMaterialize, *schema);
}

std::unordered_set<f_group_pos> LogicalOrderByFactorizationSolver::getGroupsPosToFlatten(
    const binder::expression_vector& expressions, LogicalOperator* orderByChild) {
    // We only allow orderby key(s) to be unflat, if they are all part of the same factorization
    // group and there is no other factorized group in the schema, so any payload is also unflat
    // and part of the same factorization group. The rationale for this limitation is this: (1)
    // to keep both the frontend and orderby operators simpler, we want order by to not change
    // the schema, so the input and output of order by should have the same factorization
    // structure. (2) Because orderby needs to flatten the keys to sort, if a key column that is
    // unflat is the input, we need to somehow flatten it in the factorized table. However
    // whenever we can we want to avoid adding an explicit flatten operator as this makes us
    // fall back to tuple-at-a-time processing. However in the specified limited case, we can
    // give factorized table a set of unflat vectors (all in the same datachunk/factorization
    // group), sort the table, and scan into unflat vectors, so the schema remains the same. In
    // more complicated cases, e.g., when there are 2 factorization groups, FactorizedTable
    // cannot read back a flat column into an unflat std::vector.
    if (orderByChild->getSchema()->getNumGroups() > 1) {
        std::unordered_set<f_group_pos> dependentGroupsPos;
        for (auto& expression : expressions) {
            for (auto groupPos : orderByChild->getSchema()->getDependentGroupsPos(expression)) {
                dependentGroupsPos.insert(groupPos);
            }
        }
        return FlattenAllFactorizationSolver::getGroupsPosToFlatten(
            dependentGroupsPos, orderByChild->getSchema());
    }
    return std::unordered_set<f_group_pos>{};
}

} // namespace planner
} // namespace kuzu
