#include "planner/logical_plan/logical_operator/logical_recursive_extend.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/recursive_extend/recursive_join.h"

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
    auto sharedState = createSharedState(*nbrNode, storageManager);
    auto pathPos = DataPos();
    if (extend->getJoinType() == planner::RecursiveJoinType::TRACK_PATH) {
        pathPos = DataPos(outSchema->getExpressionPos(*rel));
    }
    auto dataInfo = std::make_unique<RecursiveJoinDataInfo>(boundNodeIDPos, nbrNodeIDPos,
        nbrNode->getTableIDsSet(), lengthPos, std::move(recursivePlanResultSetDescriptor),
        recursiveDstNodeIDPos, recursiveInfo->node->getTableIDsSet(), recursiveEdgeIDPos, pathPos);
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    return std::make_unique<RecursiveJoin>(rel->getLowerBound(), rel->getUpperBound(),
        rel->getRelType(), extend->getJoinType(), sharedState, std::move(dataInfo),
        std::move(prevOperator), getOperatorID(), extend->getExpressionsForPrinting(),
        std::move(recursiveRoot));
}

} // namespace processor
} // namespace kuzu
