#include "graph/on_disk_graph.h"
#include "planner/operator/logical_gds_call.h"
#include "planner/operator/sip/logical_semi_masker.h"
#include "processor/operator/gds_call.h"
#include "processor/plan_mapper.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::graph;
using namespace kuzu::function;
using namespace kuzu::planner;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapGDSCall(const LogicalOperator* logicalOperator) {
    auto& call = logicalOperator->constCast<LogicalGDSCall>();
    auto& logicalInfo = call.getInfo();
    auto tableSchema = createFlatFTableSchema(logicalInfo.outExprs, *call.getSchema());
    auto table =
        std::make_shared<FactorizedTable>(clientContext->getMemoryManager(), tableSchema.copy());
    auto graph =
        std::make_unique<OnDiskGraph>(clientContext, logicalInfo.getBindData()->graphEntry.copy());
    auto sharedState = std::make_shared<GDSCallSharedState>(table, std::move(graph));
    auto bindData = logicalInfo.getBindData();
    if (bindData->hasNodeOutput()) {
        sharedState->setOutputNodeMask(getNodeOffsetMaskMap(*bindData->getNodeOutput()));
    }
    auto printInfo = std::make_unique<GDSCallPrintInfo>(call.getInfo().func.name);
    auto gdsAlgorithm = call.getInfo().getGDS()->copy();
    auto gdsCall = std::make_unique<GDSCall>(std::make_unique<ResultSetDescriptor>(),
        std::move(gdsAlgorithm), sharedState, getOperatorID(), std::move(printInfo));
    // Map node predicate pipeline
    if (call.hasNodePredicate()) {
        logicalOpToPhysicalOpMap.insert({logicalOperator, gdsCall.get()});
        sharedState->setGraphNodeMask(std::make_unique<NodeOffsetMaskMap>());
        auto maskMap = sharedState->getGraphNodeMaskMap();
        for (auto logicalRoot : call.getNodeMaskRoots()) {
            auto logicalSemiMasker = logicalRoot->ptrCast<LogicalSemiMasker>();
            logicalSemiMasker->addTarget(logicalOperator);
            KU_ASSERT(logicalSemiMasker->getNodeTableIDs().size() == 1);
            for (auto tableID : logicalSemiMasker->getNodeTableIDs()) {
                maskMap->addMask(tableID, getSemiMask(tableID));
            }
            auto root = mapOperator(logicalRoot.get());
            gdsCall->addChild(createDummySink(logicalRoot->getSchema(), std::move(root)));
        }
        logicalOpToPhysicalOpMap.erase(logicalOperator);
    }
    logicalOpToPhysicalOpMap.insert({logicalOperator, gdsCall.get()});
    physical_op_vector_t children;
    children.push_back(std::move(gdsCall));
    return createFTableScanAligned(logicalInfo.outExprs, call.getSchema(), table,
        DEFAULT_VECTOR_CAPACITY, std::move(children));
}

} // namespace processor
} // namespace kuzu
