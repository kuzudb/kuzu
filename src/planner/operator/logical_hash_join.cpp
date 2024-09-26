#include "planner/operator/logical_hash_join.h"

#include "common/cast.h"
#include "planner/operator/factorization/flatten_resolver.h"
#include "planner/operator/factorization/sink_util.h"
#include "planner/operator/scan/logical_scan_node_table.h"

using namespace kuzu::common;

namespace kuzu {
namespace planner {

f_group_pos_set LogicalHashJoin::getGroupsPosToFlattenOnProbeSide() {
    f_group_pos_set result;
    if (!requireFlatProbeKeys()) {
        return result;
    }
    auto probeSchema = children[0]->getSchema();
    for (auto& [probeKey, buildKey] : joinConditions) {
        result.insert(probeSchema->getGroupPos(*probeKey));
    }
    return result;
}

f_group_pos_set LogicalHashJoin::getGroupsPosToFlattenOnBuildSide() {
    auto buildSchema = children[1]->getSchema();
    f_group_pos_set joinNodesGroupPos;
    for (auto& [probeKey, buildKey] : joinConditions) {
        joinNodesGroupPos.insert(buildSchema->getGroupPos(*buildKey));
    }
    return FlattenAllButOne::getGroupsPosToFlatten(joinNodesGroupPos, *buildSchema);
}

void LogicalHashJoin::computeFactorizedSchema() {
    auto probeSchema = children[0]->getSchema();
    auto buildSchema = children[1]->getSchema();
    schema = probeSchema->copy();
    switch (joinType) {
    case JoinType::INNER:
    case JoinType::LEFT:
    case JoinType::COUNT: {
        // Populate group position mapping
        std::unordered_map<f_group_pos, f_group_pos> buildToProbeKeyGroupPositionMap;
        for (auto& [probeKey, buildKey] : joinConditions) {
            auto probeKeyGroupPos = probeSchema->getGroupPos(*probeKey);
            auto buildKeyGroupPos = buildSchema->getGroupPos(*buildKey);
            if (!buildToProbeKeyGroupPositionMap.contains(buildKeyGroupPos)) {
                buildToProbeKeyGroupPositionMap.insert({buildKeyGroupPos, probeKeyGroupPos});
            }
        }
        // Resolve expressions to materialize in each group
        binder::expression_vector expressionsToMaterializeInNonKeyGroups;
        for (auto groupIdx = 0u; groupIdx < buildSchema->getNumGroups(); ++groupIdx) {
            auto expressions = buildSchema->getExpressionsInScope(groupIdx);
            bool isKeyGroup = buildToProbeKeyGroupPositionMap.contains(groupIdx);
            if (isKeyGroup) { // merge key group
                auto probeKeyGroupPos = buildToProbeKeyGroupPositionMap.at(groupIdx);
                for (auto& expression : expressions) {
                    // Join key may repeat for internal ID based joins
                    schema->insertToGroupAndScopeMayRepeat(expression, probeKeyGroupPos);
                }
            } else {
                for (auto& expression : expressions) {
                    expressionsToMaterializeInNonKeyGroups.push_back(expression);
                }
            }
        }
        SinkOperatorUtil::mergeSchema(*buildSchema, expressionsToMaterializeInNonKeyGroups,
            *schema);
        if (mark != nullptr) {
            auto groupPos = schema->getGroupPos(*joinConditions[0].first);
            schema->insertToGroupAndScope(mark, groupPos);
        }
    } break;
    case JoinType::MARK: {
        std::unordered_set<f_group_pos> probeSideKeyGroupPositions;
        for (auto& [probeKey, buildKey] : joinConditions) {
            probeSideKeyGroupPositions.insert(probeSchema->getGroupPos(*probeKey));
        }
        if (probeSideKeyGroupPositions.size() > 1) {
            SchemaUtils::validateNoUnFlatGroup(probeSideKeyGroupPositions, *probeSchema);
        }
        auto markPos = *probeSideKeyGroupPositions.begin();
        schema->insertToGroupAndScope(mark, markPos);
    } break;
    default:
        KU_UNREACHABLE;
    }
}

void LogicalHashJoin::computeFlatSchema() {
    auto probeSchema = children[0]->getSchema();
    auto buildSchema = children[1]->getSchema();
    schema = probeSchema->copy();
    switch (joinType) {
    case JoinType::INNER:
    case JoinType::LEFT:
    case JoinType::COUNT: {
        for (auto& expression : buildSchema->getExpressionsInScope()) {
            // Join key may repeat for internal ID based joins.
            schema->insertToGroupAndScopeMayRepeat(expression, 0);
        }
        if (mark != nullptr) {
            schema->insertToGroupAndScope(mark, 0);
        }
    } break;
    case JoinType::MARK: {
        schema->insertToGroupAndScope(mark, 0);
    } break;
    default:
        KU_UNREACHABLE;
    }
}

std::string LogicalHashJoin::getExpressionsForPrinting() const {
    if (isNodeIDOnlyJoin()) {
        return binder::ExpressionUtil::toStringOrdered(getJoinNodeIDs());
    }
    return binder::ExpressionUtil::toString(joinConditions);
}

binder::expression_vector LogicalHashJoin::getExpressionsToMaterialize() const {
    switch (joinType) {
    case JoinType::INNER:
    case JoinType::LEFT:
    case JoinType::COUNT: {
        return children[1]->getSchema()->getExpressionsInScope();
    }
    case JoinType::MARK: {
        return binder::expression_vector{};
    }
    default:
        KU_UNREACHABLE;
    }
}

std::unique_ptr<LogicalOperator> LogicalHashJoin::copy() {
    auto op = std::make_unique<LogicalHashJoin>(joinConditions, joinType, mark, children[0]->copy(),
        children[1]->copy());
    op->sipInfo = sipInfo;
    return op;
}

bool LogicalHashJoin::isNodeIDOnlyJoin() const {
    for (auto& [probeKey, buildKey] : joinConditions) {
        if (probeKey->getUniqueName() != buildKey->getUniqueName() ||
            probeKey->getDataType().getLogicalTypeID() != common::LogicalTypeID::INTERNAL_ID) {
            return false;
        }
    }
    return true;
}

binder::expression_vector LogicalHashJoin::getJoinNodeIDs() const {
    binder::expression_vector result;
    KU_ASSERT(isNodeIDOnlyJoin());
    for (auto& [probeKey, _] : joinConditions) {
        result.push_back(probeKey);
    }
    return result;
}

bool LogicalHashJoin::requireFlatProbeKeys() {
    // Flatten for multiple join keys.
    if (joinConditions.size() > 1) {
        return true;
    }
    // Flatten for left join.
    if (joinType == JoinType::LEFT || joinType == JoinType::COUNT) {
        return true; // TODO(Guodong): fix this. We shouldn't require flatten.
    }
    auto& [probeKey, buildKey] = joinConditions[0];
    // Flatten for non-ID-based join.
    if (probeKey->dataType.getLogicalTypeID() != LogicalTypeID::INTERNAL_ID) {
        return true;
    }
    return !isJoinKeyUniqueOnBuildSide(*buildKey);
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
    if (op->getOperatorType() != LogicalOperatorType::SCAN_NODE_TABLE) {
        return false;
    }
    auto scan = ku_dynamic_cast<LogicalScanNodeTable*>(op);
    if (scan->getNodeID()->getUniqueName() != joinNodeID.getUniqueName()) {
        return false;
    }
    return true;
}

} // namespace planner
} // namespace kuzu
