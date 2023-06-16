#include "common/string_utils.h"
#include "planner/logical_plan/logical_operator/logical_recursive_extend.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/hash_join/hash_join_build.h"
#include "processor/operator/recursive_extend/recursive_join.h"
#include "processor/operator/recursive_extend/recursive_join_property_probe.h"
#include "processor/operator/table_scan/factorized_table_scan.h"

using namespace kuzu::binder;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

static std::shared_ptr<RecursiveJoinSharedState> createSharedState(
    const binder::NodeExpression& nbrNode, const storage::StorageManager& storageManager) {
    std::vector<std::unique_ptr<NodeOffsetSemiMask>> semiMasks;
    for (auto tableID : nbrNode.getTableIDs()) {
        auto nodeTable = storageManager.getNodesStore().getNodeTable(tableID);
        semiMasks.push_back(std::make_unique<NodeOffsetSemiMask>(nodeTable));
    }
    return std::make_shared<RecursiveJoinSharedState>(std::move(semiMasks));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalRecursiveExtendToPhysical(
    planner::LogicalOperator* logicalOperator) {
    auto extend = (LogicalRecursiveExtend*)logicalOperator;
    auto boundNode = extend->getBoundNode();
    auto nbrNode = extend->getNbrNode();
    auto rel = extend->getRel();
    auto recursiveNode = rel->getRecursiveNode();
    auto lengthExpression = rel->getLengthExpression();
    // Map recursive plan
    auto logicalRecursiveRoot = extend->getRecursiveChild();
    auto recursiveRoot = mapLogicalOperatorToPhysical(logicalRecursiveRoot);
    auto recursivePlanSchema = logicalRecursiveRoot->getSchema();
    auto recursivePlanResultSetDescriptor =
        std::make_unique<ResultSetDescriptor>(recursivePlanSchema);
    auto recursiveDstNodeIDPos =
        DataPos(recursivePlanSchema->getExpressionPos(*recursiveNode->getInternalIDProperty()));
    auto recursiveEdgeIDPos =
        DataPos(recursivePlanSchema->getExpressionPos(*rel->getInternalIDProperty()));
    // Map child plan
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    // Generate RecursiveJoin
    auto outSchema = extend->getSchema();
    auto inSchema = extend->getChild(0)->getSchema();
    auto boundNodeIDPos = DataPos(inSchema->getExpressionPos(*boundNode->getInternalIDProperty()));
    auto nbrNodeIDPos = DataPos(outSchema->getExpressionPos(*nbrNode->getInternalIDProperty()));
    auto lengthPos = DataPos(outSchema->getExpressionPos(*lengthExpression));
    auto sharedState = createSharedState(*nbrNode, storageManager);
    auto pathPos = DataPos();
    if (extend->getJoinType() == planner::RecursiveJoinType::TRACK_PATH) {
        pathPos = DataPos(outSchema->getExpressionPos(*rel));
    }
    auto dataInfo = std::make_unique<RecursiveJoinDataInfo>(boundNodeIDPos, nbrNodeIDPos,
        nbrNode->getTableIDsSet(), lengthPos, std::move(recursivePlanResultSetDescriptor),
        recursiveDstNodeIDPos, recursiveNode->getTableIDsSet(), recursiveEdgeIDPos, pathPos);
    auto recursiveJoin = std::make_unique<RecursiveJoin>(rel->getLowerBound(), rel->getUpperBound(),
        rel->getRelType(), extend->getJoinType(), sharedState, std::move(dataInfo),
        std::move(prevOperator), getOperatorID(), extend->getExpressionsForPrinting(),
        std::move(recursiveRoot));
    switch (extend->getJoinType()) {
    case planner::RecursiveJoinType::TRACK_PATH: {
        // Map build side
        auto buildPrevOperator = mapLogicalOperatorToPhysical(extend->getChild(1));
        auto buildSchema = extend->getChild(1)->getSchema();
        auto keys = expression_vector{recursiveNode->getInternalIDProperty()};
        auto payloads =
            ExpressionUtil::excludeExpressions(buildSchema->getExpressionsInScope(), keys);
        auto buildInfo = createHashBuildInfo(*buildSchema, keys, payloads);
        auto globalHashTable = std::make_unique<JoinHashTable>(
            *memoryManager, buildInfo->getNumKeys(), buildInfo->getTableSchema()->copy());
        auto hashBuildSharedState =
            std::make_shared<HashJoinSharedState>(std::move(globalHashTable));
        auto hashJoinBuild = make_unique<HashJoinBuild>(
            std::make_unique<ResultSetDescriptor>(buildSchema), hashBuildSharedState,
            std::move(buildInfo), std::move(buildPrevOperator), getOperatorID(), "");
        // Map probe
        std::unordered_map<std::string, ft_col_idx_t> propertyNameToColumnIdx;
        for (auto i = 0u; i < payloads.size(); ++i) {
            assert(payloads[i]->expressionType == common::PROPERTY);
            auto propertyName = ((PropertyExpression*)payloads[i].get())->getPropertyName();
            common::StringUtils::toUpper(propertyName);
            propertyNameToColumnIdx.insert({propertyName, i + keys.size()});
        }
        auto relDataType = rel->getDataType();
        auto nodesField =
            common::StructType::getField(&relDataType, common::InternalKeyword::NODES);
        auto nodeStructType = common::VarListType::getChildType(nodesField->getType());
        auto nodeStructFields = common::StructType::getFields(nodeStructType);
        std::vector<ft_col_idx_t> colIndicesToScan;
        for (auto i = 1u; i < nodeStructFields.size(); ++i) {
            auto field = nodeStructFields[i];
            colIndicesToScan.push_back(propertyNameToColumnIdx.at(field->getName()));
        }
        return std::make_unique<RecursiveJoinPropertyProbe>(hashBuildSharedState, pathPos,
            std::move(colIndicesToScan), std::move(recursiveJoin), std::move(hashJoinBuild),
            getOperatorID(), "");
    }
    case planner::RecursiveJoinType::TRACK_NONE: {
        return recursiveJoin;
    }
    default:
        throw common::NotImplementedException("PlanMapper::mapLogicalRecursiveExtendToPhysical");
    }
}

} // namespace processor
} // namespace kuzu
