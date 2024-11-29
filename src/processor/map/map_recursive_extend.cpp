#include "planner/operator/extend/logical_recursive_extend.h"
#include "processor/operator/recursive_extend/recursive_join.h"
#include "processor/plan_mapper.h"
#include "storage/storage_manager.h"
#include "storage/store/node_table.h"

using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

static std::shared_ptr<RecursiveJoinSharedState> createSharedState(const NodeExpression& nbrNode,
    const main::ClientContext& context) {
    std::vector<std::unique_ptr<RoaringBitmapSemiMask>> semiMasks;
    for (auto entry : nbrNode.getEntries()) {
        auto tableID = entry->getTableID();
        auto table = context.getStorageManager()->getTable(tableID)->ptrCast<storage::NodeTable>();
        semiMasks.push_back(RoaringBitmapSemiMaskUtil::createRoaringBitmapSemiMask(tableID,
            table->getNumTotalRows(context.getTx())));
    }
    return std::make_shared<RecursiveJoinSharedState>(std::move(semiMasks));
}

static ScanMultiRelTable* getScanMultiRelTable(PhysicalOperator* op) {
    if (op->getOperatorType() == PhysicalOperatorType::SCAN_REL_TABLE) {
        return dynamic_cast<ScanMultiRelTable*>(op);
    }
    KU_ASSERT(op->getNumChildren() == 1);
    return getScanMultiRelTable(op->getChild(0));
}

static std::vector<common::table_id_map_t<std::vector<size_t>>> createStepActivationRelInfos(
    RecursiveInfo* recursiveInfo, PhysicalOperator* recursiveRoot) {
    auto scanMultiRelTable = getScanMultiRelTable(recursiveRoot);
    if (!scanMultiRelTable) {
        return {};
    }

    // key:rel tableID,value:{rel index in vector,rel src tableID}
    common::table_id_map_t<std::vector<std::pair<size_t, common::table_id_t>>> temp;
    for (const auto& item : scanMultiRelTable->getScanners()) {
        auto srcTableID = item.first;

        auto& relInfos = item.second.getRelInfos();
        for (auto i = 0u; i < relInfos.size(); ++i) {
            auto tableID = relInfos.at(i).table->getTableID();
            temp[tableID].push_back({i, srcTableID});
        }
    }

    std::vector<common::table_id_map_t<std::vector<size_t>>> stepActivationRelInfos;
    for (const auto& vector : recursiveInfo->stepActivationRelInfos) {
        common::table_id_map_t<std::vector<size_t>> relTableScannerIndex;
        for (const auto& relTableID : vector) {
            for (const auto& [index, srcTableID] : temp.at(relTableID)) {
                relTableScannerIndex[srcTableID].push_back(index);
            }
        }
        stepActivationRelInfos.emplace_back(relTableScannerIndex);
    }
    return stepActivationRelInfos;
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
    auto scanMultiRelTable = getScanMultiRelTable(recursiveRoot.get());
    if (scanMultiRelTable) {
        info.stepActivationRelInfos =
            createStepActivationRelInfos(recursiveInfo, recursiveRoot.get());
    }
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    auto printInfo = std::make_unique<OPPrintInfo>();
    return std::make_unique<RecursiveJoin>(std::move(info), sharedState, std::move(prevOperator),
        getOperatorID(), std::move(recursiveRoot), std::move(printInfo));
}

} // namespace processor
} // namespace kuzu
