#include "src/planner/logical_plan/logical_operator/include/logical_intersect.h"
#include "src/processor/include/physical_plan/mapper/plan_mapper.h"
#include "src/processor/include/physical_plan/operator/hash_join/hash_join_build.h"
#include "src/processor/include/physical_plan/operator/intersect.h"

using namespace graphflow::planner;
using namespace graphflow::processor;

namespace graphflow {
namespace processor {

// Input logical plan
//            Intersect
// probe-plan scan-edge-1 scan-edge-2
//
// Output physical plan
//            Intersect
// probe-2    build-1    build-2
// probe-1    sorter     sorter
// probe-plan scan-edge-1 scan-edge-2

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalIntersectToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto logicalIntersect = (LogicalIntersect*)logicalOperator;
    auto intersectNodeID = logicalIntersect->getIntersectNode()->getIDProperty();
    vector<unique_ptr<PhysicalOperator>> children;
    vector<shared_ptr<HashJoinSharedState>> sharedStates;
    children.push_back(mapLogicalOperatorToPhysical(logicalIntersect->getChild(0), mapperContext));
    vector<DataPos> probeSideKeyVectorsPos;
    for (auto i = 1u; i < logicalIntersect->getNumChildren(); ++i) {
        auto logicalBuildChild = logicalIntersect->getChild(i);
        auto logicalBuildInfo = logicalIntersect->getBuildInfo(i - 1);
        auto key = logicalBuildInfo->key;
        auto buildSideMapperContext =
            MapperContext(make_unique<ResultSetDescriptor>(*logicalBuildInfo->schema));
        // S(a)E(e1)
        // map build child plan
        auto buildSidePrevOperator =
            mapLogicalOperatorToPhysical(logicalBuildChild, buildSideMapperContext);
        auto keyDataPos = buildSideMapperContext.getDataPos(key->getIDProperty());
        assert(keyDataPos.dataChunkPos == 0 && keyDataPos.valueVectorPos == 0);
        // add build on top. Note: payload refers to non-key in map_hash_join.cpp
        // S(a)E(e1)SORT(c)BUILD(a->c)
        vector<DataPos> payloadDataPoses;
        vector<bool> isPayloadsFlat;
        vector<DataType> payloadDataTypes;
        for (auto& expression : logicalBuildInfo->expressionsToMaterialize) {
            auto expressionName = expression->getUniqueName();
            if (expressionName == key->getIDProperty()) {
                continue;
            }
            auto payloadDataPos = buildSideMapperContext.getDataPos(expressionName);
            assert(payloadDataPos.dataChunkPos == 1 && payloadDataPos.valueVectorPos == 0);
            payloadDataPoses.push_back(payloadDataPos);
            assert(expression->dataType.typeID == NODE_ID);
            payloadDataTypes.push_back(expression->dataType);
            auto isPayloadFlat = logicalBuildInfo->schema->getGroup(expressionName)->getIsFlat();
            assert(!isPayloadFlat);
            isPayloadsFlat.push_back(isPayloadFlat);
        }
        assert(payloadDataPoses.size() == 1);
        auto buildDataInfo = BuildDataInfo(keyDataPos, payloadDataPoses, isPayloadsFlat);
        probeSideKeyVectorsPos.push_back(mapperContext.getDataPos(key->getIDProperty()));
        auto sharedState = make_shared<HashJoinSharedState>(payloadDataTypes);
        sharedStates.push_back(sharedState);
        auto hashJoinBuild = make_unique<HashJoinBuild>(sharedState, buildDataInfo,
                                                        move(buildSidePrevOperator), getOperatorID(), key->getRawName());
        children.push_back(move(hashJoinBuild));
    }
    auto outputVectorPos =
        mapperContext.getDataPos(logicalIntersect->getIntersectNode()->getIDProperty());
    return make_unique<Intersect>(outputVectorPos, probeSideKeyVectorsPos, move(sharedStates),
                                  move(children), getOperatorID(), logicalIntersect->getIntersectNode()->getUniqueName());
}

} // namespace processor
} // namespace graphflow