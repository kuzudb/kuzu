#include "planner/logical_plan/logical_operator/logical_hash_join.h"

#include "planner/logical_plan/logical_operator/logical_scan_node.h"
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
    } else { // mark join
        std::unordered_set<f_group_pos> probeSideKeyGroupPositions;
        for (auto& joinNodeID : joinNodeIDs) {
            probeSideKeyGroupPositions.insert(probeSchema->getGroupPos(*joinNodeID));
        }
        if (probeSideKeyGroupPositions.size() > 1) {
            SchemaUtils::validateNoUnFlatGroup(probeSideKeyGroupPositions, *probeSchema);
        }
        auto markPos = *probeSideKeyGroupPositions.begin();
        schema->insertToGroupAndScope(mark, markPos);
    }
}

std::unordered_set<f_group_pos>
LogicalHashJoinFactorizationResolver::getGroupsPosToFlattenOnProbeSide(
    const binder::expression_vector& joinNodeIDs, LogicalOperator* probeChild,
    LogicalOperator* buildChild) {
    std::unordered_set<f_group_pos> result;
    if (!requireFlatProbeKeys(joinNodeIDs, buildChild)) {
        return result;
    }
    auto probeSchema = probeChild->getSchema();
    for (auto& joinNodeID : joinNodeIDs) {
        result.insert(probeSchema->getGroupPos(*joinNodeID));
    }
    return result;
}

bool LogicalHashJoinFactorizationResolver::requireFlatProbeKeys(
    const binder::expression_vector& joinNodeIDs, kuzu::planner::LogicalOperator* buildChild) {
    if (joinNodeIDs.size() > 1) {
        return true;
    }
    auto joinNode = joinNodeIDs[0].get();
    return !isJoinKeyUniqueOnBuildSide(*joinNode, buildChild);
}

bool LogicalHashJoinFactorizationResolver::isJoinKeyUniqueOnBuildSide(
    const binder::Expression& joinNodeID, LogicalOperator* buildChild) {
    auto buildSchema = buildChild->getSchema();
    auto numGroupsInScope = buildSchema->getGroupsPosInScope().size();
    bool hasProjectedOutGroups = buildSchema->getNumGroups() > numGroupsInScope;
    if (numGroupsInScope > 1 || hasProjectedOutGroups) {
        return false;
    }
    // Now there is a single factorization group, we need to further make sure joinNodeID comes from
    // ScanNodeID operator. Because if joinNodeID comes from a ColExtend we cannot guarantee the
    // reverse mapping is still many-to-one. We look for the most simple pattern where build plan is
    // linear.
    auto firstop = buildChild;
    while (firstop->getNumChildren() != 0) {
        if (firstop->getNumChildren() > 1) {
            return false;
        }
        firstop = firstop->getChild(0).get();
    }
    if (firstop->getOperatorType() != LogicalOperatorType::SCAN_NODE) {
        return false;
    }
    auto scanNodeID = (LogicalScanNode*)firstop;
    if (scanNodeID->getNode()->getInternalIDPropertyName() != joinNodeID.getUniqueName()) {
        return false;
    }
    return true;
}

} // namespace planner
} // namespace kuzu
