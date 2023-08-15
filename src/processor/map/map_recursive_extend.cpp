#include "binder/expression/node_expression.h"
#include "binder/expression/rel_expression.h"
#include "common/query_rel_type.h"
#include "planner/logical_plan/extend/logical_recursive_extend.h"
#include "planner/logical_plan/logical_operator.h"
#include "processor/operator/recursive_extend/recursive_join.h"
#include "processor/operator/table_scan/factorized_table_scan.h"
#include "processor/plan_mapper.h"

using namespace kuzu::binder;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

static std::shared_ptr<RecursiveJoinSharedState> createSharedState(
    const binder::NodeExpression& nbrNode, const binder::NodeExpression& boundNode,
    const binder::RelExpression& rel, RecursiveJoinDataInfo* dataInfo, RecursiveJoinType joinType,
    const storage::StorageManager& storageManager,
    std::shared_ptr<FactorizedTableScanSharedState>& fTableSharedState) {
    std::vector<std::unique_ptr<NodeOffsetSemiMask>> semiMasks;
    for (auto tableID : nbrNode.getTableIDs()) {
        auto nodeTable = storageManager.getNodesStore().getNodeTable(tableID);
        semiMasks.push_back(std::make_unique<NodeOffsetSemiMask>(nodeTable));
    }
    std::shared_ptr<MorselDispatcher> morselDispatcher;
    bool isSingleLabel = boundNode.getTableIDs().size() == 1 && nbrNode.getTableIDs().size() == 1 &&
                         dataInfo->recursiveDstNodeTableIDs.size() == 1;
    auto maxNodeOffsetsPerTable =
        storageManager.getNodesStore().getNodesStatisticsAndDeletedIDs().getMaxNodeOffsetPerTable();
    auto maxNodeOffset = maxNodeOffsetsPerTable.at(nbrNode.getSingleTableID());
#if defined(_MSC_VER)
    morselDispatcher = std::make_shared<MorselDispatcher>(common::SchedulerType::OneThreadOneMorsel,
        rel.getLowerBound(), rel.getUpperBound(), maxNodeOffset);
#else
    if (joinType == planner::RecursiveJoinType::TRACK_NONE && isSingleLabel) {
        morselDispatcher = std::make_shared<MorselDispatcher>(common::SchedulerType::nThreadkMorsel,
            rel.getLowerBound(), rel.getUpperBound(), maxNodeOffset);
    } else {
        morselDispatcher =
            std::make_shared<MorselDispatcher>(common::SchedulerType::OneThreadOneMorsel,
                rel.getLowerBound(), rel.getUpperBound(), maxNodeOffset);
    }
#endif
    return std::make_shared<RecursiveJoinSharedState>(
        morselDispatcher, fTableSharedState, std::move(semiMasks));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapRecursiveExtend(
    planner::LogicalOperator* logicalOperator) {
    auto extend = (LogicalRecursiveExtend*)logicalOperator;
    auto boundNode = extend->getBoundNode();
    auto nbrNode = extend->getNbrNode();
    auto rel = extend->getRel();
    auto recursiveInfo = rel->getRecursiveInfo();
    auto lengthExpression = rel->getLengthExpression();
    // Map recursive plan
    auto logicalRecursiveRoot = extend->getRecursiveChild();
    auto recursiveRoot = mapOperator(logicalRecursiveRoot.get());
    auto recursivePlanSchema = logicalRecursiveRoot->getSchema();
    auto recursivePlanResultSetDescriptor =
        std::make_unique<ResultSetDescriptor>(recursivePlanSchema);
    auto recursiveDstNodeIDPos = DataPos(
        recursivePlanSchema->getExpressionPos(*recursiveInfo->nodeCopy->getInternalIDProperty()));
    auto recursiveEdgeIDPos = DataPos(
        recursivePlanSchema->getExpressionPos(*recursiveInfo->rel->getInternalIDProperty()));
    // Generate RecursiveJoin
    auto outSchema = extend->getSchema();
    auto inSchema = extend->getChild(0)->getSchema();
    auto boundNodeIDPos = DataPos(inSchema->getExpressionPos(*boundNode->getInternalIDProperty()));
    auto nbrNodeIDPos = DataPos(outSchema->getExpressionPos(*nbrNode->getInternalIDProperty()));
    auto lengthPos = DataPos(outSchema->getExpressionPos(*lengthExpression));
    auto expressions = inSchema->getExpressionsInScope();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    auto resultCollector =
        appendResultCollectorIfNotCopy(std::move(prevOperator), expressions, inSchema);
    auto sharedFTable = ((ResultCollector*)resultCollector.get())->getResultFactorizedTable();
    auto fTableSharedState = std::make_shared<FactorizedTableScanSharedState>(sharedFTable, 1u);
    auto pathPos = DataPos();
    if (extend->getJoinType() == planner::RecursiveJoinType::TRACK_PATH) {
        pathPos = DataPos(outSchema->getExpressionPos(*rel));
    }
    auto dataInfo = std::make_unique<RecursiveJoinDataInfo>(boundNodeIDPos, nbrNodeIDPos,
        nbrNode->getTableIDsSet(), lengthPos, std::move(recursivePlanResultSetDescriptor),
        recursiveDstNodeIDPos, recursiveInfo->node->getTableIDsSet(), recursiveEdgeIDPos, pathPos);
    auto sharedState = createSharedState(*nbrNode, *boundNode, *rel, dataInfo.get(),
        extend->getJoinType(), storageManager, fTableSharedState);
    std::vector<DataPos> outDataPoses;
    std::vector<uint32_t> colIndicesToScan;
    for (auto i = 0u; i < expressions.size(); ++i) {
        outDataPoses.emplace_back(outSchema->getExpressionPos(*expressions[i]));
        colIndicesToScan.push_back(i);
    }
    return std::make_unique<RecursiveJoin>(rel->getLowerBound(), rel->getUpperBound(),
        rel->getRelType(), extend->getJoinType(), sharedState, std::move(dataInfo), outDataPoses,
        colIndicesToScan, std::move(resultCollector), getOperatorID(),
        extend->getExpressionsForPrinting(), std::move(recursiveRoot));
}

} // namespace processor
} // namespace kuzu
