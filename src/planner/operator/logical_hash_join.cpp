#include "planner/logical_plan/logical_operator/logical_hash_join.h"

#include "planner/logical_plan/logical_operator/flatten_resolver.h"
#include "planner/logical_plan/logical_operator/logical_scan_node.h"
#include "planner/logical_plan/logical_operator/sink_util.h"

namespace kuzu {
namespace planner {

f_group_pos_set LogicalHashJoin::getGroupsPosToFlattenOnProbeSide() {
    f_group_pos_set result;
    if (!requireFlatProbeKeys()) {
        return result;
    }
    auto probeSchema = children[0]->getSchema();
    for (auto& joinNodeID : joinNodeIDs) {
        result.insert(probeSchema->getGroupPos(*joinNodeID));
    }
    return result;
}

f_group_pos_set LogicalHashJoin::getGroupsPosToFlattenOnBuildSide() {
    auto buildSchema = children[1]->getSchema();
    f_group_pos_set joinNodesGroupPos;
    for (auto& joinNodeID : joinNodeIDs) {
        joinNodesGroupPos.insert(buildSchema->getGroupPos(*joinNodeID));
    }
    return factorization::FlattenAllButOne::getGroupsPosToFlatten(joinNodesGroupPos, buildSchema);
}

void LogicalHashJoin::computeFactorizedSchema() {
    auto probeSchema = children[0]->getSchema();
    auto buildSchema = children[1]->getSchema();
    schema = probeSchema->copy();
    switch (joinType) {
    case common::JoinType::INNER:
    case common::JoinType::LEFT: {
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
            SchemaUtils::getExpressionsPerGroup(getExpressionsToMaterialize(), *buildSchema);
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
    } break;
    case common::JoinType::MARK: {
        std::unordered_set<f_group_pos> probeSideKeyGroupPositions;
        for (auto& joinNodeID : joinNodeIDs) {
            probeSideKeyGroupPositions.insert(probeSchema->getGroupPos(*joinNodeID));
        }
        if (probeSideKeyGroupPositions.size() > 1) {
            SchemaUtils::validateNoUnFlatGroup(probeSideKeyGroupPositions, *probeSchema);
        }
        auto markPos = *probeSideKeyGroupPositions.begin();
        schema->insertToGroupAndScope(mark, markPos);
    } break;
    default:
        throw common::NotImplementedException("HashJoin::computeFactorizedSchema()");
    }
}

void LogicalHashJoin::computeFlatSchema() {
    auto probeSchema = children[0]->getSchema();
    auto buildSchema = children[1]->getSchema();
    schema = probeSchema->copy();
    switch (joinType) {
    case common::JoinType::INNER:
    case common::JoinType::LEFT: {
        auto joinKeysSet = binder::expression_set{joinNodeIDs.begin(), joinNodeIDs.end()};
        for (auto& expression : buildSchema->getExpressionsInScope()) {
            if (joinKeysSet.contains(expression)) {
                continue;
            }
            schema->insertToGroupAndScope(expression, 0);
        }
    } break;
    case common::JoinType::MARK: {
        schema->insertToGroupAndScope(mark, 0);
    } break;
    default:
        throw common::NotImplementedException("HashJoin::computeFlatSchema()");
    }
}

binder::expression_vector LogicalHashJoin::getExpressionsToMaterialize() const {
    switch (joinType) {
    case common::JoinType::INNER:
    case common::JoinType::LEFT: {
        return children[1]->getSchema()->getExpressionsInScope();
    }
    case common::JoinType::MARK: {
        return binder::expression_vector{};
    }
    default:
        throw common::NotImplementedException("HashJoin::getExpressionsToMaterialize");
    }
}

bool LogicalHashJoin::requireFlatProbeKeys() {
    // Flatten for multiple join keys.
    if (joinNodeIDs.size() > 1) {
        return true;
    }
    // Flatten for left join.
    // TODO(Guodong): fix this.
    if (joinType == common::JoinType::LEFT) {
        return true;
    }
    auto joinNodeID = joinNodeIDs[0].get();
    return !isJoinKeyUniqueOnBuildSide(*joinNodeID);
}

bool LogicalHashJoin::isJoinKeyUniqueOnBuildSide(const binder::Expression& joinNodeID) {
    auto buildSchema = children[1]->getSchema();
    auto numGroupsInScope = buildSchema->getGroupsPosInScope().size();
    bool hasProjectedOutGroups = buildSchema->getNumGroups() > numGroupsInScope;
    if (numGroupsInScope > 1 || hasProjectedOutGroups) {
        return false;
    }
    // Now there is a single factorization group, we need to further make sure joinNodeID comes from
    // ScanNodeID operator. Because if joinNodeID comes from a ColExtend we cannot guarantee the
    // reverse mapping is still many-to-one. We look for the most simple pattern where build plan is
    // linear.
    auto op = children[1].get();
    while (op->getNumChildren() != 0) {
        if (op->getNumChildren() > 1) {
            return false;
        }
        op = op->getChild(0).get();
    }
    if (op->getOperatorType() != LogicalOperatorType::SCAN_NODE) {
        return false;
    }
    auto scanNodeID = (LogicalScanNode*)op;
    if (scanNodeID->getNode()->getInternalIDPropertyName() != joinNodeID.getUniqueName()) {
        return false;
    }
    return true;
}

} // namespace planner
} // namespace kuzu
