#include "src/planner/logical_plan/logical_operator/include/logical_hash_join.h"
#include "src/processor/include/physical_plan/mapper/plan_mapper.h"
#include "src/processor/include/physical_plan/operator/hash_join/hash_join_build.h"
#include "src/processor/include/physical_plan/operator/hash_join/hash_join_probe.h"
namespace graphflow {
namespace processor {

static void mapASPJoin(HashJoinProbe* hashJoinProbe) {
    auto tableScan = hashJoinProbe->getChild(0);
    while (tableScan->getOperatorType() != FACTORIZED_TABLE_SCAN) {
        assert(tableScan->getNumChildren() != 0);
        tableScan = tableScan->getChild(0);
    }
    assert(tableScan->getChild(0)->getOperatorType() == RESULT_COLLECTOR);
    auto resultCollector = tableScan->moveUnaryChild();
    assert(tableScan->getNumChildren() == 0);
    hashJoinProbe->addChild(move(resultCollector));
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalHashJoinToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto hashJoin = (LogicalHashJoin*)logicalOperator;
    auto buildSideMapperContext =
        MapperContext(make_unique<ResultSetDescriptor>(*hashJoin->getBuildSideSchema()));
    unique_ptr<PhysicalOperator> probeSidePrevOperator, buildSidePrevOperator;
    // If a semi mask is being passed from side A to side B. Then side B needs to be computed first
    // to make sure scanNodeID is generated before semiMasker.
    switch (hashJoin->getJoinType()) {
    case HashJoinType::S_JOIN: {
        probeSidePrevOperator = mapLogicalOperatorToPhysical(hashJoin->getChild(0), mapperContext);
        buildSidePrevOperator =
            mapLogicalOperatorToPhysical(hashJoin->getChild(1), buildSideMapperContext);
    } break;
    case HashJoinType::ASP_JOIN:
    default: {
        buildSidePrevOperator =
            mapLogicalOperatorToPhysical(hashJoin->getChild(1), buildSideMapperContext);
        probeSidePrevOperator = mapLogicalOperatorToPhysical(hashJoin->getChild(0), mapperContext);
    } break;
    }
    // Populate build side and probe side vector positions
    auto joinNodeID = hashJoin->getJoinNode()->getIDProperty();
    auto buildSideKeyIDDataPos = buildSideMapperContext.getDataPos(joinNodeID);
    auto probeSideKeyIDDataPos = mapperContext.getDataPos(joinNodeID);
    auto paramsString = hashJoin->getExpressionsForPrinting();
    vector<bool> isBuildSideNonKeyDataFlat;
    vector<DataPos> buildSideNonKeyDataPoses;
    vector<DataPos> probeSideNonKeyDataPoses;
    auto& buildSideSchema = *hashJoin->getBuildSideSchema();
    for (auto& expression : hashJoin->getExpressionsToMaterialize()) {
        auto expressionName = expression->getUniqueName();
        if (expressionName == joinNodeID) {
            continue;
        }
        mapperContext.addComputedExpressions(expressionName);
        buildSideNonKeyDataPoses.push_back(buildSideMapperContext.getDataPos(expressionName));
        isBuildSideNonKeyDataFlat.push_back(buildSideSchema.getGroup(expressionName)->getIsFlat());
        probeSideNonKeyDataPoses.push_back(mapperContext.getDataPos(expressionName));
    }

    vector<DataType> nonKeyDataPosesDataTypes(buildSideNonKeyDataPoses.size());
    for (auto i = 0u; i < buildSideNonKeyDataPoses.size(); i++) {
        auto [dataChunkPos, valueVectorPos] = buildSideNonKeyDataPoses[i];
        nonKeyDataPosesDataTypes[i] =
            buildSideSchema.getGroup(dataChunkPos)->getExpressions()[valueVectorPos]->getDataType();
    }
    auto sharedState = make_shared<HashJoinSharedState>(nonKeyDataPosesDataTypes);
    // create hashJoin build
    auto buildDataInfo =
        BuildDataInfo(buildSideKeyIDDataPos, buildSideNonKeyDataPoses, isBuildSideNonKeyDataFlat);
    auto hashJoinBuild = make_unique<HashJoinBuild>(sharedState, buildDataInfo,
        std::move(buildSidePrevOperator), getOperatorID(), paramsString);
    // create hashJoin probe
    auto probeDataInfo = ProbeDataInfo(probeSideKeyIDDataPos, probeSideNonKeyDataPoses);
    auto hashJoinProbe = make_unique<HashJoinProbe>(sharedState,
        hashJoin->getFlatOutputGroupPositions(), probeDataInfo, hashJoin->getIsOutputAFlatTuple(),
        std::move(probeSidePrevOperator), std::move(hashJoinBuild), getOperatorID(), paramsString);
    if (hashJoin->getJoinType() == planner::HashJoinType::ASP_JOIN) {
        mapASPJoin(hashJoinProbe.get());
    }
    return hashJoinProbe;
}

} // namespace processor
} // namespace graphflow
