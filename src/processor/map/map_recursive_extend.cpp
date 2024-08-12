#include "planner/operator/extend/logical_recursive_extend.h"
#include "processor/operator/recursive_extend/recursive_join.h"
#include "processor/plan_mapper.h"
#include "storage/storage_manager.h"
#include "storage/store/node_table.h"

using namespace kuzu::binder;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

static std::shared_ptr<RecursiveJoinSharedState> createSharedState(const NodeExpression& nbrNode,
    const main::ClientContext& context) {
    std::vector<std::unique_ptr<common::NodeOffsetLevelSemiMask>> semiMasks;
    for (auto entry : nbrNode.getEntries()) {
        auto tableID = entry->getTableID();
        auto table = context.getStorageManager()->getTable(tableID)->ptrCast<storage::NodeTable>();
        semiMasks.push_back(
            std::make_unique<common::NodeOffsetLevelSemiMask>(tableID, table->getNumRows()));
    }
    return std::make_shared<RecursiveJoinSharedState>(std::move(semiMasks));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapRecursiveExtend(LogicalOperator* logicalOperator) {
    auto extend = logicalOperator->constPtrCast<LogicalRecursiveExtend>();
    auto boundNode = extend->getBoundNode();
    auto nbrNode = extend->getNbrNode();
    auto rel = extend->getRel();
    auto recursiveInfo = rel->getRecursiveInfo();
    // Map recursive plan
    auto logicalRecursiveRoot = extend->getRecursiveChild();
    auto recursiveRoot = mapOperator(logicalRecursiveRoot.get());
    auto recursivePlanSchema = logicalRecursiveRoot->getSchema();
    // Generate RecursiveJoin
    auto outSchema = extend->getSchema();
    auto inSchema = extend->getChild(0)->getSchema();
    auto sharedState = createSharedState(*nbrNode, *clientContext);
    // Data info
    auto dataInfo = RecursiveJoinDataInfo();
    dataInfo.srcNodePos = getDataPos(*boundNode->getInternalID(), *inSchema);
    dataInfo.dstNodePos = getDataPos(*nbrNode->getInternalID(), *outSchema);
    dataInfo.dstNodeTableIDs = nbrNode->getTableIDsSet();
    dataInfo.pathLengthPos = getDataPos(*rel->getLengthExpression(), *outSchema);
    dataInfo.localResultSetDescriptor = std::make_unique<ResultSetDescriptor>(recursivePlanSchema);
    dataInfo.recursiveSrcNodeIDPos =
        getDataPos(*recursiveInfo->node->getInternalID(), *recursivePlanSchema);
    dataInfo.recursiveNodePredicateExecFlagPos =
        getDataPos(*recursiveInfo->nodePredicateExecFlag, *recursivePlanSchema);
    dataInfo.recursiveDstNodeIDPos =
        getDataPos(*recursiveInfo->nodeCopy->getInternalID(), *recursivePlanSchema);
    dataInfo.recursiveDstNodeTableIDs = recursiveInfo->node->getTableIDsSet();
    dataInfo.recursiveEdgeIDPos =
        getDataPos(*recursiveInfo->rel->getInternalIDProperty(), *recursivePlanSchema);
    if (recursiveInfo->rel->getDirectionType() == RelDirectionType::BOTH) {
        dataInfo.recursiveEdgeDirectionPos =
            getDataPos(*recursiveInfo->rel->getDirectionExpr(), *recursivePlanSchema);
    }
    if (extend->getJoinType() == RecursiveJoinType::TRACK_PATH) {
        dataInfo.pathPos = getDataPos(*rel, *outSchema);
    } else {
        dataInfo.pathPos = DataPos::getInvalidPos();
    }
    for (auto& entry : clientContext->getCatalog()->getTableEntries(clientContext->getTx())) {
        dataInfo.tableIDToName.insert({entry->getTableID(), entry->getName()});
    }
    // Info
    auto info = RecursiveJoinInfo();
    info.dataInfo = std::move(dataInfo);
    info.lowerBound = rel->getLowerBound();
    info.upperBound = rel->getUpperBound();
    info.queryRelType = rel->getRelType();
    info.joinType = extend->getJoinType();
    info.direction = extend->getDirection();
    info.extendFromSource = extend->extendFromSourceNode();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    auto printInfo = std::make_unique<OPPrintInfo>();
    return std::make_unique<RecursiveJoin>(std::move(info), sharedState, std::move(prevOperator),
        getOperatorID(), std::move(recursiveRoot), std::move(printInfo));
}

} // namespace processor
} // namespace kuzu
