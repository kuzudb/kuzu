#include "planner/join_order/join_order_util.h"

namespace kuzu {
namespace planner {

uint64_t JoinOrderUtil::getJoinKeysFlatCardinality(
    const binder::expression_vector& joinNodeIDs, const LogicalPlan& buildPlan) {
    auto schema = buildPlan.getSchema();
    f_group_pos_set unFlatGroupsPos;
    for (auto& joinID : joinNodeIDs) {
        auto groupPos = schema->getGroupPos(*joinID);
        if (!schema->getGroup(groupPos)->isFlat()) {
            unFlatGroupsPos.insert(groupPos);
        }
    }
    auto cardinality = buildPlan.getCardinality();
    for (auto groupPos : unFlatGroupsPos) {
        cardinality *= schema->getGroup(groupPos)->getMultiplier();
    }
    return cardinality;
}

} // namespace planner
} // namespace kuzu
