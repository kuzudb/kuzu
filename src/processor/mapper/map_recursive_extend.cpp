#include "common/string_utils.h"
#include "planner/logical_plan/logical_operator/logical_recursive_extend.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/hash_join/hash_join_build.h"
#include "processor/operator/recursive_extend/path_property_probe.h"
#include "processor/operator/recursive_extend/recursive_join.h"
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

static std::vector<ft_col_idx_t> getColIdxToScan(
    const expression_vector& payloads, uint32_t numKeys, const common::LogicalType& structType) {
    std::unordered_map<std::string, ft_col_idx_t> propertyNameToColumnIdx;
    for (auto i = 0u; i < payloads.size(); ++i) {
        assert(payloads[i]->expressionType == common::PROPERTY);
        auto propertyName = ((PropertyExpression*)payloads[i].get())->getPropertyName();
        common::StringUtils::toUpper(propertyName);
        propertyNameToColumnIdx.insert({propertyName, i + numKeys});
    }
    auto nodeStructFields = common::StructType::getFields(&structType);
    std::vector<ft_col_idx_t> colIndicesToScan;
    for (auto i = 1u; i < nodeStructFields.size(); ++i) {
        auto field = nodeStructFields[i];
        colIndicesToScan.push_back(propertyNameToColumnIdx.at(field->getName()));
    }
    return colIndicesToScan;
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalRecursiveExtendToPhysical(
    planner::LogicalOperator* logicalOperator) {
    auto extend = (LogicalRecursiveExtend*)logicalOperator;
    auto boundNode = extend->getBoundNode();
    auto nbrNode = extend->getNbrNode();
    auto rel = extend->getRel();
    auto recursiveInfo = rel->getRecursiveInfo();
    auto lengthExpression = rel->getLengthExpression();
    // Map recursive plan
    auto logicalRecursiveRoot = extend->getRecursiveChild();
    auto recursiveRoot = mapLogicalOperatorToPhysical(logicalRecursiveRoot);
    auto recursivePlanSchema = logicalRecursiveRoot->getSchema();
    auto recursivePlanResultSetDescriptor =
        std::make_unique<ResultSetDescriptor>(recursivePlanSchema);
    auto recursiveDstNodeIDPos = DataPos(
        recursivePlanSchema->getExpressionPos(*recursiveInfo->node->getInternalIDProperty()));
    auto recursiveEdgeIDPos = DataPos(
        recursivePlanSchema->getExpressionPos(*recursiveInfo->rel->getInternalIDProperty()));
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
        recursiveDstNodeIDPos, recursiveInfo->node->getTableIDsSet(), recursiveEdgeIDPos, pathPos);
    auto recursiveJoin = std::make_unique<RecursiveJoin>(rel->getLowerBound(), rel->getUpperBound(),
        rel->getRelType(), extend->getJoinType(), sharedState, std::move(dataInfo),
        std::move(prevOperator), getOperatorID(), extend->getExpressionsForPrinting(),
        std::move(recursiveRoot));
    switch (extend->getJoinType()) {
    case planner::RecursiveJoinType::TRACK_PATH: {
        // Map build node property
        auto nodeBuildPrevOperator = mapLogicalOperatorToPhysical(extend->getChild(1));
        auto nodeBuildSchema = extend->getChild(1)->getSchema();
        auto nodeKeys = expression_vector{recursiveInfo->node->getInternalIDProperty()};
        auto nodePayloads =
            ExpressionUtil::excludeExpressions(nodeBuildSchema->getExpressionsInScope(), nodeKeys);
        auto nodeBuildInfo = createHashBuildInfo(*nodeBuildSchema, nodeKeys, nodePayloads);
        auto nodeHashTable = std::make_unique<JoinHashTable>(
            *memoryManager, nodeBuildInfo->getNumKeys(), nodeBuildInfo->getTableSchema()->copy());
        auto nodeBuildSharedState = std::make_shared<HashJoinSharedState>(std::move(nodeHashTable));
        auto nodeBuild = make_unique<HashJoinBuild>(
            std::make_unique<ResultSetDescriptor>(nodeBuildSchema), nodeBuildSharedState,
            std::move(nodeBuildInfo), std::move(nodeBuildPrevOperator), getOperatorID(), "");
        // Map build rel property
        auto relBuildPrvOperator = mapLogicalOperatorToPhysical(extend->getChild(2));
        auto relBuildSchema = extend->getChild(2)->getSchema();
        auto relKeys = expression_vector{recursiveInfo->rel->getInternalIDProperty()};
        auto relPayloads =
            ExpressionUtil::excludeExpressions(relBuildSchema->getExpressionsInScope(), relKeys);
        auto relBuildInfo = createHashBuildInfo(*relBuildSchema, relKeys, relPayloads);
        auto relHashTable = std::make_unique<JoinHashTable>(
            *memoryManager, relBuildInfo->getNumKeys(), relBuildInfo->getTableSchema()->copy());
        auto relBuildSharedState = std::make_shared<HashJoinSharedState>(std::move(relHashTable));
        auto relBuild = std::make_unique<HashJoinBuild>(
            std::make_unique<ResultSetDescriptor>(relBuildSchema), relBuildSharedState,
            std::move(relBuildInfo), std::move(relBuildPrvOperator), getOperatorID(), "");
        // Map probe
        auto relDataType = rel->getDataType();
        auto nodesField =
            common::StructType::getField(&relDataType, common::InternalKeyword::NODES);
        auto nodeStructType = common::VarListType::getChildType(nodesField->getType());
        auto nodeColIndicesToScan = getColIdxToScan(nodePayloads, nodeKeys.size(), *nodeStructType);
        auto relsField = common::StructType::getField(&relDataType, common::InternalKeyword::RELS);
        auto relStructType = common::VarListType::getChildType(relsField->getType());
        auto relColIndicesToScan = getColIdxToScan(relPayloads, relKeys.size(), *relStructType);
        auto pathProbeInfo = std::make_unique<PathPropertyProbeDataInfo>(
            pathPos, std::move(nodeColIndicesToScan), std::move(relColIndicesToScan));
        auto pathProbeSharedState = std::make_shared<PathPropertyProbeSharedState>(
            nodeBuildSharedState, relBuildSharedState);
        std::vector<std::unique_ptr<PhysicalOperator>> children;
        children.push_back(std::move(recursiveJoin));
        children.push_back(std::move(nodeBuild));
        children.push_back(std::move(relBuild));
        return std::make_unique<PathPropertyProbe>(std::move(pathProbeInfo), pathProbeSharedState,
            std::move(children), getOperatorID(), "");
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
