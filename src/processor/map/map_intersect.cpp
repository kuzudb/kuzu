#include "binder/expression/expression_util.h"
#include "planner/operator/logical_intersect.h"
#include "processor/operator/intersect/intersect.h"
#include "processor/operator/intersect/intersect_build.h"
#include "processor/plan_mapper.h"

using namespace kuzu::binder;
using namespace kuzu::planner;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapIntersect(LogicalOperator* logicalOperator) {
    auto logicalIntersect = (LogicalIntersect*)logicalOperator;
    auto intersectNodeID = logicalIntersect->getIntersectNodeID();
    auto outSchema = logicalIntersect->getSchema();
    std::vector<std::unique_ptr<PhysicalOperator>> children;
    children.resize(logicalOperator->getNumChildren());
    std::vector<std::shared_ptr<HashJoinSharedState>> sharedStates;
    std::vector<IntersectDataInfo> intersectDataInfos;
    // Map build side children.
    for (auto i = 1u; i < logicalIntersect->getNumChildren(); i++) {
        auto keyNodeID = logicalIntersect->getKeyNodeID(i - 1);
        auto keys = expression_vector{keyNodeID};
        auto keyTypes = ExpressionUtil::getDataTypes(keys);
        auto buildSchema = logicalIntersect->getChild(i)->getSchema();
        auto buildPrevOperator = mapOperator(logicalIntersect->getChild(i).get());
        auto payloadExpressions =
            binder::ExpressionUtil::excludeExpressions(buildSchema->getExpressionsInScope(), keys);
        auto buildInfo = createHashBuildInfo(*buildSchema, keys, payloadExpressions);
        auto globalHashTable = std::make_unique<JoinHashTable>(
            *memoryManager, LogicalType::copy(keyTypes), buildInfo->getTableSchema()->copy());
        auto sharedState = std::make_shared<HashJoinSharedState>(std::move(globalHashTable));
        sharedStates.push_back(sharedState);
        children[i] = make_unique<IntersectBuild>(
            std::make_unique<ResultSetDescriptor>(buildSchema), sharedState, std::move(buildInfo),
            std::move(buildPrevOperator), getOperatorID(), keyNodeID->toString());
        // Collect intersect info
        std::vector<DataPos> vectorsToScanPos;
        auto expressionsToScan = binder::ExpressionUtil::excludeExpressions(
            buildSchema->getExpressionsInScope(), {keyNodeID, intersectNodeID});
        for (auto& expression : expressionsToScan) {
            vectorsToScanPos.emplace_back(outSchema->getExpressionPos(*expression));
        }
        IntersectDataInfo info{DataPos(outSchema->getExpressionPos(*keyNodeID)), vectorsToScanPos};
        intersectDataInfos.push_back(info);
    }
    // Map probe side child.
    children[0] = mapOperator(logicalIntersect->getChild(0).get());
    // Map intersect.
    auto outputDataPos =
        DataPos(outSchema->getExpressionPos(*logicalIntersect->getIntersectNodeID()));
    auto intersect = make_unique<Intersect>(outputDataPos, intersectDataInfos, sharedStates,
        std::move(children), getOperatorID(), logicalIntersect->getExpressionsForPrinting());
    if (logicalIntersect->getSIP() == SidewaysInfoPassing::PROBE_TO_BUILD) {
        mapSIPJoin(intersect.get());
    }
    return intersect;
}

} // namespace processor
} // namespace kuzu
