#include "src/planner/logical_plan/logical_operator/include/logical_hash_join.h"
#include "src/processor/include/physical_plan/mapper/plan_mapper.h"
#include "src/processor/include/physical_plan/operator/hash_join/hash_join_build.h"
#include "src/processor/include/physical_plan/operator/hash_join/hash_join_probe.h"

namespace graphflow {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalHashJoinToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto hashJoin = (LogicalHashJoin*)logicalOperator;
    auto buildSideMapperContext =
        MapperContext(make_unique<ResultSetDescriptor>(*hashJoin->getBuildSideSchema()));
    auto buildSidePrevOperator =
        mapLogicalOperatorToPhysical(hashJoin->getChild(1), buildSideMapperContext);
    auto probeSidePrevOperator = mapLogicalOperatorToPhysical(hashJoin->getChild(0), mapperContext);
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
    auto probeSideOperator = probeSidePrevOperator.get();
    while (probeSideOperator->getNumChildren() > 0) {
        probeSideOperator = probeSideOperator->getChild(0);
    }
    auto probeSideScanNode = (ScanNodeID*)probeSideOperator;
    probeSideScanNode->getSharedState()->setHasSemiMask();
    // create hashJoin build
    auto buildDataInfo =
        BuildDataInfo(buildSideKeyIDDataPos, buildSideNonKeyDataPoses, isBuildSideNonKeyDataFlat);
    auto hashJoinBuild =
        make_unique<HashJoinBuild>(sharedState, buildDataInfo, move(buildSidePrevOperator),
            getOperatorID(), paramsString, probeSideScanNode->getSharedState());
    // create hashJoin probe
    auto probeDataInfo = ProbeDataInfo(probeSideKeyIDDataPos, probeSideNonKeyDataPoses);
    auto hashJoinProbe = make_unique<HashJoinProbe>(sharedState,
        hashJoin->getFlatOutputGroupPositions(), probeDataInfo, move(probeSidePrevOperator),
        move(hashJoinBuild), getOperatorID(), paramsString);
    return hashJoinProbe;
}

} // namespace processor
} // namespace graphflow
