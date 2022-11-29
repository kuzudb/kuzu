#include "planner/logical_plan/logical_operator/logical_intersect.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/intersect/intersect.h"
#include "processor/operator/intersect/intersect_build.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalIntersectToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto logicalIntersect = (LogicalIntersect*)logicalOperator;
    vector<unique_ptr<PhysicalOperator>> children;
    // 0th child is the probe child. others are build children.
    children.push_back(mapLogicalOperatorToPhysical(logicalIntersect->getChild(0), mapperContext));
    vector<shared_ptr<IntersectSharedState>> sharedStates;
    vector<IntersectDataInfo> intersectDataInfos;
    // Map build side children.
    for (auto i = 1u; i < logicalIntersect->getNumChildren(); i++) {
        auto buildInfo = logicalIntersect->getBuildInfo(i - 1);
        auto buildKey = buildInfo->key->getInternalIDPropertyName();
        auto buildSideSchema = buildInfo->schema.get();
        auto buildSideMapperContext =
            MapperContext(make_unique<ResultSetDescriptor>(*buildSideSchema));
        auto buildSidePrevOperator =
            mapLogicalOperatorToPhysical(logicalIntersect->getChild(i), buildSideMapperContext);
        vector<DataType> payloadsDataTypes;
        vector<DataPos> payloadsDataPos;
        auto buildDataInfo = generateBuildDataInfo(mapperContext, buildInfo->schema.get(),
            {buildInfo->key}, buildInfo->expressionsToMaterialize);
        for (auto& [dataPos, _] : buildDataInfo.payloadsPosAndType) {
            auto expression = buildSideSchema->getGroup(dataPos.dataChunkPos)
                                  ->getExpressions()[dataPos.valueVectorPos];
            if (expression->getUniqueName() ==
                logicalIntersect->getIntersectNode()->getInternalIDPropertyName()) {
                continue;
            }
            payloadsDataPos.push_back(mapperContext.getDataPos(expression->getUniqueName()));
            payloadsDataTypes.push_back(expression->getDataType());
        }
        auto sharedState = make_shared<IntersectSharedState>();
        sharedStates.push_back(sharedState);
        children.push_back(make_unique<IntersectBuild>(sharedState, buildDataInfo,
            std::move(buildSidePrevOperator), getOperatorID(), buildKey));
        IntersectDataInfo info{
            mapperContext.getDataPos(buildKey), payloadsDataPos, payloadsDataTypes};
        intersectDataInfos.push_back(info);
    }
    // Map intersect.
    auto outputDataPos =
        mapperContext.getDataPos(logicalIntersect->getIntersectNode()->getInternalIDPropertyName());
    return make_unique<Intersect>(outputDataPos, intersectDataInfos, sharedStates, move(children),
        getOperatorID(), logicalIntersect->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
