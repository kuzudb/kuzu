#include "graph/on_disk_graph.h"
#include "planner/operator/extend/logical_recursive_extend.h"
#include "planner/operator/sip/logical_semi_masker.h"
#include "processor/operator/recursive_extend.h"
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
    auto& bindData = extend.getBindData();
    auto columns = extend.getResultColumns();
    auto tableSchema = createFlatFTableSchema(columns, *extend.getSchema());
    auto table =
        std::make_shared<FactorizedTable>(clientContext->getMemoryManager(), tableSchema.copy());
    auto graph = std::make_unique<OnDiskGraph>(clientContext, bindData.graphEntry.copy());
    auto sharedState =
        std::make_shared<RecursiveExtendSharedState>(table, std::move(graph), extend.getLimitNum());
    if (extend.hasNbrTableIDSet()) {
        sharedState->setNbrTableIDSet(extend.getNbrTableIDSet());
    }
    sharedState->setInputNodeMask(createNodeOffsetMaskMap(*bindData.nodeInput));
    sharedState->setOutputNodeMask(createNodeOffsetMaskMap(*bindData.nodeOutput));
    auto printInfo =
        std::make_unique<RecursiveExtendPrintInfo>(extend.getFunction().getFunctionName());
    auto descriptor = std::make_unique<ResultSetDescriptor>();
    auto recursiveExtend = std::make_unique<RecursiveExtend>(std::move(descriptor),
        extend.getFunction().copy(), bindData, sharedState, getOperatorID(), std::move(printInfo));
    // Map node predicate pipeline
    if (extend.hasNodePredicate()) {
        logicalOpToPhysicalOpMap.insert({logicalOperator, recursiveExtend.get()});
        sharedState->setPathNodeMask(std::make_unique<NodeOffsetMaskMap>());
        auto maskMap = sharedState->getPathNodeMaskMap();
        auto logicalRoot = extend.getNodeMaskRoot();
        KU_ASSERT(logicalRoot->getNumChildren() == 1);
        auto child = logicalRoot->getChild(0);
        KU_ASSERT(child->getOperatorType() == LogicalOperatorType::SEMI_MASKER);
        auto logicalSemiMasker = child->ptrCast<LogicalSemiMasker>();
        logicalSemiMasker->addTarget(logicalOperator);
        for (auto tableID : logicalSemiMasker->getNodeTableIDs()) {
            maskMap->addMask(tableID, createSemiMask(tableID));
        }
        auto root = mapOperator(logicalRoot.get());
        recursiveExtend->addChild(std::move(root));
        logicalOpToPhysicalOpMap.erase(logicalOperator);
    }
    logicalOpToPhysicalOpMap.insert({logicalOperator, recursiveExtend.get()});
    physical_op_vector_t children;
    children.push_back(std::move(recursiveExtend));
    return createFTableScanAligned(columns, extend.getSchema(), table, DEFAULT_VECTOR_CAPACITY,
        std::move(children));
}

} // namespace processor
} // namespace kuzu
