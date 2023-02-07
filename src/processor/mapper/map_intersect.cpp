#include "planner/logical_plan/logical_operator/logical_intersect.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/intersect/intersect.h"
#include "processor/operator/intersect/intersect_build.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalIntersectToPhysical(
    LogicalOperator* logicalOperator) {
    auto logicalIntersect = (LogicalIntersect*)logicalOperator;
    auto outSchema = logicalIntersect->getSchema();
    std::vector<std::unique_ptr<PhysicalOperator>> children;
    // 0th child is the probe child. others are build children.
    children.push_back(mapLogicalOperatorToPhysical(logicalIntersect->getChild(0)));
    std::vector<std::shared_ptr<IntersectSharedState>> sharedStates;
    std::vector<IntersectDataInfo> intersectDataInfos;
    // Map build side children.
    for (auto i = 1u; i < logicalIntersect->getNumChildren(); i++) {
        auto buildInfo = logicalIntersect->getBuildInfo(i - 1);
        auto buildSchema = logicalIntersect->getChild(i)->getSchema();
        auto buildSidePrevOperator = mapLogicalOperatorToPhysical(logicalIntersect->getChild(i));
        std::vector<DataPos> payloadsDataPos;
        auto buildDataInfo = generateBuildDataInfo(
            *buildSchema, {buildInfo->keyNodeID}, buildInfo->expressionsToMaterialize);
        for (auto& [dataPos, _] : buildDataInfo.payloadsPosAndType) {
            auto expression = buildSchema->getGroup(dataPos.dataChunkPos)
                                  ->getExpressions()[dataPos.valueVectorPos];
            if (expression->getUniqueName() ==
                logicalIntersect->getIntersectNodeID()->getUniqueName()) {
                continue;
            }
            payloadsDataPos.emplace_back(outSchema->getExpressionPos(*expression));
        }
        auto sharedState = std::make_shared<IntersectSharedState>();
        sharedStates.push_back(sharedState);
        children.push_back(make_unique<IntersectBuild>(
            std::make_unique<ResultSetDescriptor>(*buildSchema), sharedState, buildDataInfo,
            std::move(buildSidePrevOperator), getOperatorID(), buildInfo->keyNodeID->getRawName()));
        IntersectDataInfo info{
            DataPos(outSchema->getExpressionPos(*buildInfo->keyNodeID)), payloadsDataPos};
        intersectDataInfos.push_back(info);
    }
    // Map intersect.
    auto outputDataPos =
        DataPos(outSchema->getExpressionPos(*logicalIntersect->getIntersectNodeID()));
    return make_unique<Intersect>(outputDataPos, intersectDataInfos, sharedStates,
        std::move(children), getOperatorID(), logicalIntersect->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
