#include "common/query_rel_type.h"
#include "planner/logical_plan/logical_operator/logical_recursive_extend.h"
#include "planner/logical_plan/logical_operator/recursive_join_type.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/recursive_extend/recursive_join.h"

using namespace kuzu::binder;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

static std::shared_ptr<RecursiveJoinSharedState> createSharedState(
    const binder::NodeExpression& nbrNode, const storage::StorageManager& storageManager,
    std::shared_ptr<FTableSharedState>& fTableSharedState) {
    std::vector<std::unique_ptr<NodeOffsetSemiMask>> semiMasks;
    for (auto tableID : nbrNode.getTableIDs()) {
        auto nodeTable = storageManager.getNodesStore().getNodeTable(tableID);
        semiMasks.push_back(std::make_unique<NodeOffsetSemiMask>(nodeTable));
    }
    return std::make_shared<RecursiveJoinSharedState>(fTableSharedState, std::move(semiMasks));
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
    // Generate RecursiveJoin
    auto outSchema = extend->getSchema();
    auto inSchema = extend->getChild(0)->getSchema();
    auto boundNodeIDPos = DataPos(inSchema->getExpressionPos(*boundNode->getInternalIDProperty()));
    auto nbrNodeIDPos = DataPos(outSchema->getExpressionPos(*nbrNode->getInternalIDProperty()));
    auto lengthPos = DataPos(outSchema->getExpressionPos(*lengthExpression));
    auto expressions = inSchema->getExpressionsInScope();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    auto resultCollector = appendResultCollector(expressions, inSchema, std::move(prevOperator));
    auto sharedFTable = resultCollector->getSharedState();
    auto sharedState = createSharedState(*nbrNode, storageManager, sharedFTable);
    auto pathPos = DataPos();
    if (extend->getJoinType() == planner::RecursiveJoinType::TRACK_PATH) {
        pathPos = DataPos(outSchema->getExpressionPos(*rel));
    }
    auto dataInfo = std::make_unique<RecursiveJoinDataInfo>(boundNodeIDPos, nbrNodeIDPos,
        nbrNode->getTableIDsSet(), lengthPos, std::move(recursivePlanResultSetDescriptor),
        recursiveDstNodeIDPos, recursiveInfo->node->getTableIDsSet(), recursiveEdgeIDPos, pathPos);
    sharedFTable->setMaxMorselSize(1);
    std::vector<DataPos> outDataPoses;
    std::vector<uint32_t> colIndicesToScan;
    for (auto i = 0u; i < expressions.size(); ++i) {
        outDataPoses.emplace_back(outSchema->getExpressionPos(*expressions[i]));
        colIndicesToScan.push_back(i);
    }
    std::vector<std::unique_ptr<NodeOffsetSemiMask>> semiMasks;
    for (auto tableID : nbrNode->getTableIDs()) {
        auto nodeTable = storageManager.getNodesStore().getNodeTable(tableID);
        semiMasks.push_back(std::make_unique<NodeOffsetSemiMask>(nodeTable));
    }
    std::shared_ptr<MorselDispatcher> morselDispatcher;
    bool checkSingleLabel = boundNode->getTableIDs().size() == 1 &&
                            nbrNode->getTableIDs().size() == 1 &&
                            dataInfo->recursiveDstNodeTableIDs.size() == 1;
    if (rel->getRelType() == common::QueryRelType::SHORTEST &&
        extend->getJoinType() == planner::RecursiveJoinType::TRACK_NONE && checkSingleLabel) {
        auto maxNodeOffsetsPerTable = storageManager.getNodesStore()
                                          .getNodesStatisticsAndDeletedIDs()
                                          .getMaxNodeOffsetPerTable();
        auto maxNodeOffset = maxNodeOffsetsPerTable.at(nbrNode->getSingleTableID());
        morselDispatcher = std::make_shared<MorselDispatcher>(SchedulerType::nThreadkMorsel,
            rel->getLowerBound(), rel->getUpperBound(), maxNodeOffset, numThreadsForExecution);
    } else {
        morselDispatcher = std::make_shared<MorselDispatcher>(SchedulerType::OneThreadOneMorsel,
            rel->getLowerBound(), rel->getUpperBound(), UINT64_MAX /* maxNodeOffset */,
            numThreadsForExecution);
    }
    return std::make_unique<RecursiveJoin>(rel->getLowerBound(), rel->getUpperBound(),
        rel->getRelType(), extend->getJoinType(), sharedState, std::move(dataInfo), outDataPoses,
        colIndicesToScan, morselDispatcher, std::move(resultCollector), getOperatorID(),
        extend->getExpressionsForPrinting(), std::move(recursiveRoot));
}

} // namespace processor
} // namespace kuzu
