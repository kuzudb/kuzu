#include "planner/logical_plan/logical_operator/logical_hash_join.h"
#include "planner/logical_plan/logical_operator/logical_semi_masker.h"
#include "planner/logical_plan/logical_plan_util.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/hash_join/hash_join_build.h"
#include "processor/operator/hash_join/hash_join_probe.h"
#include "processor/operator/scan_node_id.h"
#include "processor/operator/semi_masker.h"
#include "processor/operator/table_scan/factorized_table_scan.h"

using namespace kuzu::binder;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

static bool containASPOnPipeline(LogicalHashJoin* logicalHashJoin) {
    auto op = logicalHashJoin->getChild(0); // check probe side
    while (op->getNumChildren() == 1) {     // check pipeline
        if (op->getOperatorType() == LogicalOperatorType::SEMI_MASKER) {
            return true;
        }
        op = op->getChild(0);
    }
    return false;
}

static bool containFTableScan(LogicalHashJoin* logicalHashJoin) {
    auto root = logicalHashJoin->getChild(1).get(); // check build side
    return !LogicalPlanUtil::collectOperators(root, LogicalOperatorType::FTABLE_SCAN).empty();
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

static void mapASPJoin(Expression* joinNodeID, HashJoinProbe* hashJoinProbe) {
    // fetch scan node ID on build side
    auto hashJoinBuild = hashJoinProbe->getChild(1);
    assert(hashJoinBuild->getOperatorType() == PhysicalOperatorType::HASH_JOIN_BUILD);
    std::vector<ScanNodeID*> scanNodeIDCandidates;
    for (auto& op :
        PhysicalPlanUtil::collectOperators(hashJoinBuild, PhysicalOperatorType::SCAN_NODE_ID)) {
        auto scanNodeID = (ScanNodeID*)op;
        if (scanNodeID->getNodeID() == joinNodeID->getUniqueName()) {
            scanNodeIDCandidates.push_back(scanNodeID);
        }
    }
    assert(scanNodeIDCandidates.size() == 1);
    // set semi masker
    auto tableScan = getTableScanForAccHashJoin(hashJoinProbe);
    assert(tableScan->getChild(0)->getChild(0)->getOperatorType() ==
           PhysicalOperatorType::SEMI_MASKER);
    auto semiMasker = (SemiMasker*)tableScan->getChild(0)->getChild(0);
    auto sharedState = scanNodeIDCandidates[0]->getSharedState();
    assert(sharedState->getNumTableStates() == 1);
    semiMasker->setSharedState(sharedState->getTableState(0));
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
    std::vector<PhysicalOperator*> tableScanCandidates;
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

BuildDataInfo PlanMapper::generateBuildDataInfo(const Schema& buildSideSchema,
    const expression_vector& keys, const expression_vector& payloads) {
    std::vector<std::pair<DataPos, common::DataType>> buildKeysPosAndType, buildPayloadsPosAndTypes;
    std::vector<bool> isBuildPayloadsFlat, isBuildPayloadsInKeyChunk;
    std::vector<bool> isBuildDataChunkContainKeys(buildSideSchema.getNumGroups(), false);
    std::unordered_set<std::string> joinKeyNames;
    for (auto& key : keys) {
        auto buildSideKeyPos = DataPos(buildSideSchema.getExpressionPos(*key));
        isBuildDataChunkContainKeys[buildSideKeyPos.dataChunkPos] = true;
        buildKeysPosAndType.emplace_back(buildSideKeyPos, common::INTERNAL_ID);
        joinKeyNames.insert(key->getUniqueName());
    }
    for (auto& payload : payloads) {
        if (joinKeyNames.find(payload->getUniqueName()) != joinKeyNames.end()) {
            continue;
        }
        auto payloadPos = DataPos(buildSideSchema.getExpressionPos(*payload));
        buildPayloadsPosAndTypes.emplace_back(payloadPos, payload->dataType);
        auto payloadGroup = buildSideSchema.getGroup(payloadPos.dataChunkPos);
        isBuildPayloadsFlat.push_back(payloadGroup->isFlat());
        isBuildPayloadsInKeyChunk.push_back(isBuildDataChunkContainKeys[payloadPos.dataChunkPos]);
    }
    return BuildDataInfo(buildKeysPosAndType, buildPayloadsPosAndTypes, isBuildPayloadsFlat,
        isBuildPayloadsInKeyChunk);
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalHashJoinToPhysical(
    LogicalOperator* logicalOperator) {
    auto hashJoin = (LogicalHashJoin*)logicalOperator;
    auto outSchema = hashJoin->getSchema();
    auto buildSchema = hashJoin->getBuildSideSchema();
    auto buildSidePrevOperator = mapLogicalOperatorToPhysical(hashJoin->getChild(1));
    auto probeSidePrevOperator = mapLogicalOperatorToPhysical(hashJoin->getChild(0));
    // Populate build side and probe side std::vector positions
    auto paramsString = hashJoin->getExpressionsForPrinting();
    auto buildDataInfo = generateBuildDataInfo(
        *buildSchema, hashJoin->getJoinNodeIDs(), hashJoin->getExpressionsToMaterialize());
    std::vector<DataPos> probeKeysDataPos;
    for (auto& joinNodeID : hashJoin->getJoinNodeIDs()) {
        probeKeysDataPos.emplace_back(outSchema->getExpressionPos(*joinNodeID));
    }
    std::vector<DataPos> probePayloadsOutPos;
    for (auto& [dataPos, _] : buildDataInfo.payloadsPosAndType) {
        auto expression =
            buildSchema->getGroup(dataPos.dataChunkPos)->getExpressions()[dataPos.valueVectorPos];
        probePayloadsOutPos.emplace_back(outSchema->getExpressionPos(*expression));
    }
    auto sharedState = std::make_shared<HashJoinSharedState>();
    // create hashJoin build
    auto hashJoinBuild =
        make_unique<HashJoinBuild>(std::make_unique<ResultSetDescriptor>(*buildSchema), sharedState,
            buildDataInfo, std::move(buildSidePrevOperator), getOperatorID(), paramsString);
    // create hashJoin probe
    ProbeDataInfo probeDataInfo(probeKeysDataPos, probePayloadsOutPos);
    if (hashJoin->getJoinType() == common::JoinType::MARK) {
        auto mark = hashJoin->getMark();
        auto markOutputPos = DataPos(outSchema->getExpressionPos(*mark));
        probeDataInfo.markDataPos = markOutputPos;
    }
    auto hashJoinProbe = make_unique<HashJoinProbe>(sharedState, hashJoin->getJoinType(),
        probeDataInfo, std::move(probeSidePrevOperator), std::move(hashJoinBuild), getOperatorID(),
        paramsString);
    if (hashJoin->getIsProbeAcc()) {
        if (containASPOnPipeline(hashJoin)) {
            mapASPJoin(hashJoin->getJoinNodeIDs()[0].get(), hashJoinProbe.get());
        } else {
            assert(containFTableScan(hashJoin));
            mapAccJoin(hashJoinProbe.get());
        }
    }
    return hashJoinProbe;
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalSemiMaskerToPhysical(
    LogicalOperator* logicalOperator) {
    auto logicalSemiMasker = (LogicalSemiMasker*)logicalOperator;
    auto inSchema = logicalSemiMasker->getChild(0)->getSchema();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    auto keyDataPos = DataPos(inSchema->getExpressionPos(*logicalSemiMasker->getNodeID()));
    return make_unique<SemiMasker>(keyDataPos, std::move(prevOperator), getOperatorID(),
        logicalSemiMasker->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
