#include "src/planner/logical_plan/logical_operator/include/logical_hash_join.h"
#include "src/processor/include/physical_plan/mapper/plan_mapper.h"
#include "src/processor/include/physical_plan/operator/hash_join/hash_join_build.h"
#include "src/processor/include/physical_plan/operator/hash_join/hash_join_probe.h"

namespace graphflow {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalHashJoinToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto& hashJoin = (const LogicalHashJoin&)*logicalOperator;
    auto buildSideMapperContext =
        MapperContext(make_unique<ResultSetDescriptor>(*hashJoin.getBuildSideSchema()));
    auto buildSidePrevOperator =
        mapLogicalOperatorToPhysical(hashJoin.getChild(1), buildSideMapperContext);
    auto probeSidePrevOperator = mapLogicalOperatorToPhysical(hashJoin.getChild(0), mapperContext);
    // Populate build side and probe side vector positions
    auto buildSideKeyIDDataPos = buildSideMapperContext.getDataPos(hashJoin.getJoinNodeID());
    auto probeSideKeyIDDataPos = mapperContext.getDataPos(hashJoin.getJoinNodeID());
    auto paramsString = hashJoin.getExpressionsForPrinting();
    vector<bool> isBuildSideNonKeyDataFlat;
    vector<DataPos> buildSideNonKeyDataPoses;
    vector<DataPos> probeSideNonKeyDataPoses;
    auto& buildSideSchema = *hashJoin.getBuildSideSchema();
    for (auto& expression : hashJoin.getExpressionsToMaterialize()) {
        auto expressionName = expression->getUniqueName();
        if (expressionName == hashJoin.getJoinNodeID()) {
            continue;
        }
        mapperContext.addComputedExpressions(expressionName);
        buildSideNonKeyDataPoses.push_back(buildSideMapperContext.getDataPos(expressionName));
        isBuildSideNonKeyDataFlat.push_back(buildSideSchema.getGroup(expressionName)->getIsFlat());
        probeSideNonKeyDataPoses.push_back(mapperContext.getDataPos(expressionName));
    }

    auto sharedState = make_shared<HashJoinSharedState>();
    // create hashJoin build
    auto buildDataInfo =
        BuildDataInfo(buildSideKeyIDDataPos, buildSideNonKeyDataPoses, isBuildSideNonKeyDataFlat);
    auto hashJoinBuild = make_unique<HashJoinBuild>(
        sharedState, buildDataInfo, move(buildSidePrevOperator), getOperatorID(), paramsString);
    // create hashJoin probe
    auto probeDataInfo = ProbeDataInfo(probeSideKeyIDDataPos, probeSideNonKeyDataPoses);
    auto hashJoinProbe = make_unique<HashJoinProbe>(sharedState,
        hashJoin.getFlatOutputGroupPositions(), probeDataInfo, move(probeSidePrevOperator),
        move(hashJoinBuild), getOperatorID(), paramsString);
    return hashJoinProbe;
}

} // namespace processor
} // namespace graphflow
