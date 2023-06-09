#include "planner/logical_plan/logical_operator/logical_recursive_extend.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/recursive_extend/recursive_join.h"
#include "processor/operator/table_scan/factorized_table_scan.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalRecursiveExtendToPhysical(
    planner::LogicalOperator* logicalOperator) {
    auto extend = (LogicalRecursiveExtend*)logicalOperator;
    auto boundNode = extend->getBoundNode();
    auto nbrNode = extend->getNbrNode();
    auto rel = extend->getRel();
    auto recursiveNode = rel->getRecursiveNode();
    auto lengthExpression = rel->getLengthExpression();
    // map recursive plan
    auto logicalRecursiveRoot = extend->getRecursivePlanRoot();
    auto recursiveRoot = mapLogicalOperatorToPhysical(logicalRecursiveRoot);
    auto recursivePlanSchema = logicalRecursiveRoot->getSchema();
    auto recursivePlanResultSetDescriptor =
        std::make_unique<ResultSetDescriptor>(recursivePlanSchema);
    auto recursiveDstNodeIDPos =
        DataPos(recursivePlanSchema->getExpressionPos(*recursiveNode->getInternalIDProperty()));
    auto recursiveEdgeIDPos =
        DataPos(recursivePlanSchema->getExpressionPos(*rel->getInternalIDProperty()));
    // map child plan
    auto outSchema = extend->getSchema();
    auto inSchema = extend->getChild(0)->getSchema();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    auto expressions = inSchema->getExpressionsInScope();
    auto resultCollector = appendResultCollector(expressions, inSchema, std::move(prevOperator));
    auto sharedFTable = resultCollector->getSharedState();
    sharedFTable->setMaxMorselSize(1);
    std::vector<DataPos> outDataPoses;
    std::vector<uint32_t> colIndicesToScan;
    for (auto i = 0u; i < expressions.size(); ++i) {
        outDataPoses.emplace_back(outSchema->getExpressionPos(*expressions[i]));
        colIndicesToScan.push_back(i);
    }
    auto fTableScan = make_unique<FactorizedTableScan>(std::move(outDataPoses),
        std::move(colIndicesToScan), sharedFTable, std::move(resultCollector), getOperatorID(), "");
    // Generate RecursiveJoinDataInfo
    auto boundNodeIDVectorPos =
        DataPos(inSchema->getExpressionPos(*boundNode->getInternalIDProperty()));
    auto nbrNodeIDVectorPos =
        DataPos(outSchema->getExpressionPos(*nbrNode->getInternalIDProperty()));
    auto lengthVectorPos = DataPos(outSchema->getExpressionPos(*lengthExpression));
    std::unique_ptr<RecursiveJoinDataInfo> dataInfo;
    switch (extend->getJoinType()) {
    case planner::RecursiveJoinType::TRACK_PATH: {
        auto pathVectorPos = DataPos(outSchema->getExpressionPos(*rel));
        dataInfo = std::make_unique<RecursiveJoinDataInfo>(boundNodeIDVectorPos, nbrNodeIDVectorPos,
            nbrNode->getTableIDsSet(), lengthVectorPos, std::move(recursivePlanResultSetDescriptor),
            recursiveDstNodeIDPos, recursiveNode->getTableIDsSet(), recursiveEdgeIDPos,
            pathVectorPos);
    } break;
    case planner::RecursiveJoinType::TRACK_NONE: {
        dataInfo = std::make_unique<RecursiveJoinDataInfo>(boundNodeIDVectorPos, nbrNodeIDVectorPos,
            nbrNode->getTableIDsSet(), lengthVectorPos, std::move(recursivePlanResultSetDescriptor),
            recursiveDstNodeIDPos, recursiveNode->getTableIDsSet(), recursiveEdgeIDPos);
    } break;
    default:
        throw common::NotImplementedException("PlanMapper::mapLogicalRecursiveExtendToPhysical");
    }
    std::vector<std::unique_ptr<NodeOffsetSemiMask>> semiMasks;
    for (auto tableID : nbrNode->getTableIDs()) {
        auto nodeTable = storageManager.getNodesStore().getNodeTable(tableID);
        semiMasks.push_back(std::make_unique<NodeOffsetSemiMask>(nodeTable));
    }
    auto sharedState = std::make_shared<RecursiveJoinSharedState>(std::move(semiMasks));
    return std::make_unique<RecursiveJoin>(rel->getLowerBound(), rel->getUpperBound(),
        rel->getRelType(), extend->getJoinType(), sharedState, std::move(dataInfo),
        std::move(fTableScan), getOperatorID(), extend->getExpressionsForPrinting(),
        std::move(recursiveRoot));
}

} // namespace processor
} // namespace kuzu
