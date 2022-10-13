#include "src/planner/logical_plan/logical_operator/include/logical_intersect.h"
#include "src/processor/include/physical_plan/mapper/plan_mapper.h"
#include "src/processor/include/physical_plan/operator/intersect/intersect.h"
#include "src/processor/include/physical_plan/operator/intersect/intersect_build.h"

using namespace graphflow::planner;

namespace graphflow {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalIntersectToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto logicalIntersect = (LogicalIntersect*)logicalOperator;
    vector<unique_ptr<PhysicalOperator>> children;
    // 0th child is the probe child. others are build children.
    children.push_back(mapLogicalOperatorToPhysical(logicalIntersect->getChild(0), mapperContext));
    vector<shared_ptr<IntersectSharedState>> sharedStates;
    vector<DataPos> probeKeysDataPos;
    // Map build side children.
    for (auto i = 1u; i < logicalIntersect->getNumChildren(); i++) {
        auto buildInfo = logicalIntersect->getBuildInfo(i - 1);
        auto buildKey = buildInfo->key->getIDProperty();
        auto buildSideSchema = buildInfo->schema.get();
        auto buildSideMapperContext =
            MapperContext(make_unique<ResultSetDescriptor>(*buildSideSchema));
        auto buildSidePrevOperator =
            mapLogicalOperatorToPhysical(logicalIntersect->getChild(i), buildSideMapperContext);
        vector<DataType> payloadsDataTypes;
        auto buildDataInfo = generateBuildDataInfo(mapperContext, buildInfo->schema.get(),
            {buildInfo->key}, buildInfo->expressionsToMaterialize);
        for (auto& dataPos : buildDataInfo.payloadsDataPos) {
            payloadsDataTypes.push_back(buildSideSchema->getGroup(dataPos.dataChunkPos)
                                            ->getExpressions()[dataPos.valueVectorPos]
                                            ->getDataType());
        }
        auto sharedState = make_shared<IntersectSharedState>(payloadsDataTypes);
        sharedStates.push_back(sharedState);
        children.push_back(make_unique<IntersectBuild>(sharedState, buildDataInfo,
            std::move(buildSidePrevOperator), getOperatorID(), buildKey));
        probeKeysDataPos.push_back(
            mapperContext.getDataPos(logicalIntersect->getBuildInfo(i - 1)->key->getIDProperty()));
    }
    // Map intersect.
    auto outputDataPos =
        mapperContext.getDataPos(logicalIntersect->getIntersectNode()->getIDProperty());
    return make_unique<Intersect>(outputDataPos, probeKeysDataPos, sharedStates, move(children),
        getOperatorID(), logicalIntersect->getExpressionsForPrinting());
}

} // namespace processor
} // namespace graphflow
