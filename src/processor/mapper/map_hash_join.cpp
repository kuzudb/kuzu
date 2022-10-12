#include "include/plan_mapper.h"

#include "src/planner/logical_plan/include/logical_plan_util.h"
#include "src/planner/logical_plan/logical_operator/include/logical_hash_join.h"
#include "src/planner/logical_plan/logical_operator/include/logical_semi_masker.h"
#include "src/processor/operator/hash_join/include/hash_join_build.h"
#include "src/processor/operator/hash_join/include/hash_join_probe.h"
#include "src/processor/operator/include/scan_node_id.h"
#include "src/processor/operator/include/semi_masker.h"
#include "src/processor/operator/table_scan/include/factorized_table_scan.h"

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

BuildDataInfo PlanMapper::generateBuildDataInfo(MapperContext& mapperContext,
    Schema* buildSideSchema, const vector<shared_ptr<NodeExpression>>& keys,
    const expression_vector& payloads) {
    auto buildSideMapperContext = MapperContext(make_unique<ResultSetDescriptor>(*buildSideSchema));
    vector<DataPos> buildKeysDataPos, buildPayloadsDataPos;
    vector<bool> isBuildPayloadsFlat, isBuildPayloadsInKeyChunk;
    vector<bool> isBuildDataChunkContainKeys(
        buildSideMapperContext.getResultSetDescriptor()->getNumDataChunks(), false);
    unordered_set<string> joinNodeIDs;
    for (auto& key : keys) {
        auto nodeID = key->getIDProperty();
        auto buildSideKeyPos = buildSideMapperContext.getDataPos(nodeID);
        isBuildDataChunkContainKeys[buildSideKeyPos.dataChunkPos] = true;
        buildKeysDataPos.push_back(buildSideMapperContext.getDataPos(nodeID));
        joinNodeIDs.emplace(nodeID);
    }
    for (auto& payload : payloads) {
        auto payloadUniqueName = payload->getUniqueName();
        if (joinNodeIDs.find(payloadUniqueName) != joinNodeIDs.end()) {
            continue;
        }
        mapperContext.addComputedExpressions(payloadUniqueName);
        auto payloadPos = buildSideMapperContext.getDataPos(payloadUniqueName);
        buildPayloadsDataPos.push_back(payloadPos);
        auto payloadGroup = buildSideSchema->getGroup(payloadPos.dataChunkPos);
        isBuildPayloadsFlat.push_back(payloadGroup->getIsFlat());
        isBuildPayloadsInKeyChunk.push_back(isBuildDataChunkContainKeys[payloadPos.dataChunkPos]);
    }
    return BuildDataInfo(
        buildKeysDataPos, buildPayloadsDataPos, isBuildPayloadsFlat, isBuildPayloadsInKeyChunk);
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
    auto buildSideSchema = hashJoin->getBuildSideSchema();
    auto buildDataInfo = generateBuildDataInfo(mapperContext, buildSideSchema,
        hashJoin->getJoinNodes(), hashJoin->getExpressionsToMaterialize());
    vector<DataPos> probeKeysDataPos, probePayloadsDataPos;
    vector<DataType> payloadsDataTypes;
    for (auto& joinNode : hashJoin->getJoinNodes()) {
        auto joinNodeID = joinNode->getIDProperty();
        probeKeysDataPos.push_back(mapperContext.getDataPos(joinNodeID));
    }
    for (auto& dataPos : buildDataInfo.payloadsDataPos) {
        auto expression = buildSideSchema->getGroup(dataPos.dataChunkPos)
                              ->getExpressions()[dataPos.valueVectorPos];
        payloadsDataTypes.push_back(expression->getDataType());
        probePayloadsDataPos.push_back(mapperContext.getDataPos(expression->getUniqueName()));
    }
    auto sharedState = make_shared<HashJoinSharedState>(payloadsDataTypes);
    // create hashJoin build
    auto hashJoinBuild = make_unique<HashJoinBuild>(sharedState, buildDataInfo,
        std::move(buildSidePrevOperator), getOperatorID(), paramsString);
    // create hashJoin probe
    ProbeDataInfo probeDataInfo(probeKeysDataPos, probePayloadsDataPos);
    if (hashJoin->getJoinType() == JoinType::MARK) {
        auto mark = hashJoin->getMark();
        auto markOutputPos = mapperContext.getDataPos(mark->getUniqueName());
        mapperContext.addComputedExpressions(mark->getUniqueName());
        probeDataInfo.markDataPos = markOutputPos;
    }
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
