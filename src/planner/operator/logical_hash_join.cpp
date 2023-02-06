#include "planner/logical_plan/logical_operator/logical_hash_join.h"

#include "planner/logical_plan/logical_operator/sink_util.h"

namespace kuzu {
namespace planner {

void LogicalHashJoin::computeSchema() {
    auto probeSchema = children[0]->getSchema();
    auto buildSchema = children[1]->getSchema();
    schema = probeSchema->copy();
    if (joinType != common::JoinType::MARK) {
        // resolve key groups
        std::unordered_map<f_group_pos, std::unordered_set<std::string>> keyGroupPosToKeys;
        for (auto& joinNodeID : joinNodeIDs) {
            auto groupPos = buildSchema->getGroupPos(*joinNodeID);
            if (!keyGroupPosToKeys.contains(groupPos)) {
                keyGroupPosToKeys.insert({groupPos, std::unordered_set<std::string>()});
            }
            keyGroupPosToKeys.at(groupPos).insert(joinNodeID->getUniqueName());
        }
        // resolve expressions to materialize in each group
        auto expressionsToMaterializePerGroup =
            SchemaUtils::getExpressionsPerGroup(expressionsToMaterialize, *buildSchema);
        binder::expression_vector expressionsToMaterializeInNonKeyGroups;
        for (auto i = 0; i < buildSchema->getNumGroups(); ++i) {
            auto expressions = expressionsToMaterializePerGroup[i];
            bool isKeyGroup = keyGroupPosToKeys.contains(i);
            if (isKeyGroup) { // merge key group
                auto keys = keyGroupPosToKeys.at(i);
                auto resultGroupPos = schema->getGroupPos(*keys.begin());
                for (auto& expression : expressions) {
                    if (keys.contains(expression->getUniqueName())) {
                        continue;
                    }
                    schema->insertToGroupAndScope(expression, resultGroupPos);
                }
            } else {
                for (auto& expression : expressions) {
                    expressionsToMaterializeInNonKeyGroups.push_back(expression);
                }
            }
        }
        SinkOperatorUtil::mergeSchema(
            *buildSchema, expressionsToMaterializeInNonKeyGroups, *schema);
    } else {
        schema->insertToGroupAndScope(mark, markPos);
    }
}

} // namespace planner
} // namespace kuzu
