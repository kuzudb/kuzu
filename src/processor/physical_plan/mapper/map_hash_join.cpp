#include "src/planner/logical_plan/include/logical_plan_util.h"
#include "src/planner/logical_plan/logical_operator/include/logical_hash_join.h"
#include "src/planner/logical_plan/logical_operator/include/logical_semi_masker.h"
#include "src/processor/include/physical_plan/mapper/plan_mapper.h"
#include "src/processor/include/physical_plan/operator/factorized_table_scan.h"
#include "src/processor/include/physical_plan/operator/hash_join/hash_join_build.h"
#include "src/processor/include/physical_plan/operator/hash_join/hash_join_probe.h"
#include "src/processor/include/physical_plan/operator/scan_node_id.h"
#include "src/processor/include/physical_plan/operator/semi_masker.h"

namespace graphflow {
namespace processor {

static bool containASPOnPipeline(LogicalHashJoin* logicalHashJoin) {
    auto op = logicalHashJoin->getChild(0); // check probe side
    while (op->getNumChildren() == 1) {     // check pipeline
        if (op->getLogicalOperatorType() == LogicalOperatorType::LOGICAL_SEMI_MASKER) {
            return true;
        }
        op = op->getChild(0);
    }
    return false;
}

static bool containFTableScan(LogicalHashJoin* logicalHashJoin) {
    auto root = logicalHashJoin->getChild(1).get(); // check build side
    return !LogicalPlanUtil::collectOperators(root, LogicalOperatorType::LOGICAL_FTABLE_SCAN)
                .empty();
}

static FactorizedTableScan* getTableScanForAccHashJoin(HashJoinProbe* hashJoinProbe) {
    auto op = hashJoinProbe->getChild(0);
    while (op->getOperatorType() == PhysicalOperatorType::FLATTEN) {
        op = op->getChild(0);
    }
    assert(op->getOperatorType() == PhysicalOperatorType::FACTORIZED_TABLE_SCAN);
    return (FactorizedTableScan*)op;
}

static void constructAccPipeline(FactorizedTableScan* tableScan, HashJoinProbe* hashJoinProbe) {
    auto resultCollector = tableScan->moveUnaryChild();
    hashJoinProbe->addChild(std::move(resultCollector));
}

static void mapASPJoin(NodeExpression* joinNode, HashJoinProbe* hashJoinProbe) {
    // fetch scan node ID on build side
    auto hashJoinBuild = hashJoinProbe->getChild(1);
    assert(hashJoinBuild->getOperatorType() == PhysicalOperatorType::HASH_JOIN_BUILD);
    vector<ScanNodeID*> scanNodeIDCandidates;
    for (auto& op :
        PhysicalPlanUtil::collectOperators(hashJoinBuild, PhysicalOperatorType::SCAN_NODE_ID)) {
        auto scanNodeID = (ScanNodeID*)op;
        if (scanNodeID->getNodeName() == joinNode->getUniqueName()) {
            scanNodeIDCandidates.push_back(scanNodeID);
        }
    }
    assert(scanNodeIDCandidates.size() == 1);
    auto sharedState = scanNodeIDCandidates[0]->getSharedState();
    // set semi masker
    auto tableScan = getTableScanForAccHashJoin(hashJoinProbe);
    assert(tableScan->getChild(0)->getChild(0)->getOperatorType() ==
           PhysicalOperatorType::SEMI_MASKER);
    auto semiMasker = (SemiMasker*)tableScan->getChild(0)->getChild(0);
    semiMasker->setSharedState(sharedState);
    constructAccPipeline(tableScan, hashJoinProbe);
}

static void mapAccJoin(HashJoinProbe* hashJoinProbe) {
    auto hashJoinBuild = hashJoinProbe->getChild(1);
    assert(hashJoinBuild->getOperatorType() == PhysicalOperatorType::HASH_JOIN_BUILD);
    // fetch factorized table on probe side
    auto tableScan = getTableScanForAccHashJoin(hashJoinProbe);
    assert(tableScan->getChild(0)->getOperatorType() == PhysicalOperatorType::RESULT_COLLECTOR);
    auto resultCollector = (ResultCollector*)tableScan->getChild(0);
    auto sharedState = resultCollector->getSharedState();
    // fetch fTableScan on build side
    vector<PhysicalOperator*> tableScanCandidates;
    for (auto& op : PhysicalPlanUtil::collectOperators(
             hashJoinBuild, PhysicalOperatorType::FACTORIZED_TABLE_SCAN)) {
        if (op->getNumChildren() == 0) {
            tableScanCandidates.push_back(op);
        }
    }
    // This might not be true for nested exists subquery.
    assert(tableScanCandidates.size() == 1);
    auto tableScanCandidate = (FactorizedTableScan*)tableScanCandidates[0];
    tableScanCandidate->setSharedState(sharedState);
    constructAccPipeline(tableScan, hashJoinProbe);
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalHashJoinToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto hashJoin = (LogicalHashJoin*)logicalOperator;
    auto buildSideMapperContext =
        MapperContext(make_unique<ResultSetDescriptor>(*hashJoin->getBuildSideSchema()));
    auto buildSidePrevOperator =
        mapLogicalOperatorToPhysical(hashJoin->getChild(1), buildSideMapperContext);
    auto probeSidePrevOperator = mapLogicalOperatorToPhysical(hashJoin->getChild(0), mapperContext);
    // Populate build side and probe side vector positions
    auto paramsString = hashJoin->getExpressionsForPrinting();
    unordered_set<string> joinNodeIDs;
    vector<bool> isBuildDataChunkContainKeys(
        buildSideMapperContext.getResultSetDescriptor()->getNumDataChunks(), false);
    vector<DataPos> buildKeysDataPos;
    vector<DataPos> probeKeysDataPos;
    for (auto& joinNode : hashJoin->getJoinNodes()) {
        auto joinNodeID = joinNode->getIDProperty();
        auto buildSideKeyPos = buildSideMapperContext.getDataPos(joinNodeID);
        isBuildDataChunkContainKeys[buildSideKeyPos.dataChunkPos] = true;
        buildKeysDataPos.push_back(buildSideMapperContext.getDataPos(joinNodeID));
        probeKeysDataPos.push_back(mapperContext.getDataPos(joinNodeID));
        joinNodeIDs.emplace(joinNodeID);
    }
    vector<DataPos> buildPayloadsPos;
    vector<DataPos> probePayloadsPos;
    vector<bool> isBuildPayloadsFlat;
    vector<bool> isBuildPayloadsInKeyChunk;
    auto& buildSideSchema = *hashJoin->getBuildSideSchema();
    for (auto& expression : hashJoin->getExpressionsToMaterialize()) {
        auto expressionName = expression->getUniqueName();
        if (joinNodeIDs.find(expressionName) != joinNodeIDs.end()) {
            continue;
        }
        mapperContext.addComputedExpressions(expressionName);
        auto buildPayloadPos = buildSideMapperContext.getDataPos(expressionName);
        buildPayloadsPos.push_back(buildPayloadPos);
        isBuildPayloadsFlat.push_back(buildSideSchema.getGroup(expressionName)->getIsFlat());
        isBuildPayloadsInKeyChunk.push_back(
            isBuildDataChunkContainKeys[buildPayloadPos.dataChunkPos]);
        probePayloadsPos.push_back(mapperContext.getDataPos(expressionName));
    }

    vector<DataType> nonKeyDataPosesDataTypes(buildPayloadsPos.size());
    for (auto i = 0u; i < buildPayloadsPos.size(); i++) {
        auto [dataChunkPos, valueVectorPos] = buildPayloadsPos[i];
        nonKeyDataPosesDataTypes[i] =
            buildSideSchema.getGroup(dataChunkPos)->getExpressions()[valueVectorPos]->getDataType();
    }
    auto sharedState = make_shared<HashJoinSharedState>(nonKeyDataPosesDataTypes);
    // create hashJoin build
    auto buildDataInfo = BuildDataInfo(
        buildKeysDataPos, buildPayloadsPos, isBuildPayloadsFlat, isBuildPayloadsInKeyChunk);
    auto hashJoinBuild = make_unique<HashJoinBuild>(sharedState, buildDataInfo,
        std::move(buildSidePrevOperator), getOperatorID(), paramsString);
    // create hashJoin probe
    auto probeDataInfo = ProbeDataInfo(probeKeysDataPos, probePayloadsPos);
    auto hashJoinProbe = make_unique<HashJoinProbe>(sharedState, hashJoin->getJoinType(),
        hashJoin->getFlatOutputGroupPositions(), probeDataInfo, std::move(probeSidePrevOperator),
        std::move(hashJoinBuild), getOperatorID(), paramsString);
    if (hashJoin->getIsProbeAcc()) {
        if (containASPOnPipeline(hashJoin)) {
            mapASPJoin(hashJoin->getJoinNodes()[0].get(), hashJoinProbe.get());
        } else {
            assert(containFTableScan(hashJoin));
            mapAccJoin(hashJoinProbe.get());
        }
    }
    // TODO(Guodong): below is the skeleton code for mark join mapping
    //    if (hashJoin->getJoinType() == JoinType::MARK) {
    //        auto mark = hashJoin->getMark();
    //        auto markOutputPos = mapperContext.getDataPos(mark->getUniqueName());
    //        mapperContext.addComputedExpressions(mark->getUniqueName());
    //    }
    return hashJoinProbe;
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalSemiMaskerToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto logicalSemiMasker = (LogicalSemiMasker*)logicalOperator;
    auto node = logicalSemiMasker->getNode();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext);
    auto keyDataPos = mapperContext.getDataPos(node->getIDProperty());
    return make_unique<SemiMasker>(keyDataPos, std::move(prevOperator), getOperatorID(),
        logicalSemiMasker->getExpressionsForPrinting());
}

} // namespace processor
} // namespace graphflow
