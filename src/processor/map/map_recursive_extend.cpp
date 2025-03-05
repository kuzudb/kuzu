#include "graph/on_disk_graph.h"
#include "planner/operator/extend/logical_recursive_extend.h"
#include "planner/operator/sip/logical_semi_masker.h"
#include "processor/operator/gds_call.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;
using namespace kuzu::graph;
using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapRecursiveExtend(
    const LogicalOperator* logicalOperator) {
    auto& extend = logicalOperator->constCast<LogicalRecursiveExtend>();
    auto& logicalInfo = extend.getInfo();
    auto tableSchema = createFlatFTableSchema(logicalInfo.outExprs, *extend.getSchema());
    auto table =
        std::make_shared<FactorizedTable>(clientContext->getMemoryManager(), tableSchema.copy());
    auto graph =
        std::make_unique<OnDiskGraph>(clientContext, logicalInfo.getBindData()->graphEntry.copy());
    auto sharedState =
        std::make_shared<GDSCallSharedState>(table, std::move(graph), extend.getLimitNum());
    if (extend.hasNbrTableIDSet()) {
        sharedState->setNbrTableIDSet(extend.getNbrTableIDSet());
    }
    auto bindData = logicalInfo.getBindData();
    if (bindData->hasNodeInput()) {
        sharedState->setInputNodeMask(getNodeOffsetMaskMap(*bindData->getNodeInput()));
    }
    if (bindData->hasNodeOutput()) {
        sharedState->setOutputNodeMask(getNodeOffsetMaskMap(*bindData->getNodeOutput()));
    }
    auto printInfo = std::make_unique<GDSCallPrintInfo>(logicalInfo.func.name);
    auto gdsAlgorithm = logicalInfo.getGDS()->copy();
    auto descriptor = std::make_unique<ResultSetDescriptor>();
    auto gdsCall = std::make_unique<GDSCall>(std::move(descriptor), std::move(gdsAlgorithm),
        sharedState, getOperatorID(), std::move(printInfo));
    // Map node predicate pipeline
    if (extend.hasNodePredicate()) {
        logicalOpToPhysicalOpMap.insert({logicalOperator, gdsCall.get()});
        sharedState->setPathNodeMask(std::make_unique<NodeOffsetMaskMap>());
        auto maskMap = sharedState->getPathNodeMaskMap();
        auto logicalRoot = extend.getNodeMaskRoot();
        auto logicalSemiMasker = logicalRoot->ptrCast<LogicalSemiMasker>();
        logicalSemiMasker->addTarget(logicalOperator);
        for (auto tableID : logicalSemiMasker->getNodeTableIDs()) {
            maskMap->addMask(tableID, getSemiMask(tableID));
        }
        auto root = mapOperator(logicalRoot.get());
        gdsCall->addChild(createDummySink(logicalRoot->getSchema(), std::move(root)));
        logicalOpToPhysicalOpMap.erase(logicalOperator);
    }
    logicalOpToPhysicalOpMap.insert({logicalOperator, gdsCall.get()});
    physical_op_vector_t children;
    children.push_back(std::move(gdsCall));
    return createFTableScanAligned(logicalInfo.outExprs, extend.getSchema(), table,
        DEFAULT_VECTOR_CAPACITY, std::move(children));
}

} // namespace processor
} // namespace kuzu
