#include "binder/expression/node_expression.h"
#include "graph/on_disk_graph.h"
#include "planner/operator/logical_gds_call.h"
#include "planner/operator/sip/logical_semi_masker.h"
#include "processor/operator/gds_call.h"
#include "processor/plan_mapper.h"
#include "storage/storage_manager.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::graph;
using namespace kuzu::function;
using namespace kuzu::planner;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

static std::unique_ptr<NodeOffsetMaskMap> getNodeOffsetMaskMap(
    const std::vector<table_id_t>& tableIDs, StorageManager* storageManager) {
    auto map = std::make_unique<NodeOffsetMaskMap>();
    for (auto tableID : tableIDs) {
        auto nodeTable = storageManager->getTable(tableID)->ptrCast<NodeTable>();
        map->addMask(tableID,
            std::make_unique<NodeOffsetLevelSemiMask>(tableID, nodeTable->getNumRows()));
    }
    return map;
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapGDSCall(LogicalOperator* logicalOperator) {
    auto& call = logicalOperator->constCast<LogicalGDSCall>();
    auto outSchema = call.getSchema();
    auto tableSchema = std::make_unique<FactorizedTableSchema>();
    auto columns = call.getInfo().outExprs;
    for (auto& expr : columns) {
        auto dataPos = getDataPos(*expr, *outSchema);
        auto columnSchema = ColumnSchema(false /* isUnFlat */, dataPos.dataChunkPos,
            LogicalTypeUtils::getRowLayoutSize(expr->getDataType()));
        tableSchema->appendColumn(std::move(columnSchema));
    }
    auto table =
        std::make_shared<FactorizedTable>(clientContext->getMemoryManager(), tableSchema->copy());
    auto graph = std::make_unique<OnDiskGraph>(clientContext, call.getInfo().graphEntry);
    auto storageManager = clientContext->getStorageManager();
    auto sharedState = std::make_shared<GDSCallSharedState>(table, std::move(graph));
    common::table_id_map_t<std::unique_ptr<NodeOffsetLevelSemiMask>> masks;
    if (call.getInfo().getBindData()->hasNodeInput()) {
        // Generate an empty semi mask which later on picked by SemiMaker.
        auto& node =
            call.getInfo().getBindData()->getNodeInput()->constCast<binder::NodeExpression>();
        sharedState->setInputNodeMask(getNodeOffsetMaskMap(node.getTableIDs(), storageManager));
    }

    auto printInfo = std::make_unique<GDSCallPrintInfo>(call.getInfo().func.name);
    auto gdsAlgorithm = call.getInfo().getGDS()->copy();
    auto descriptor = std::make_unique<ResultSetDescriptor>();
    auto gdsCall = std::make_unique<GDSCall>(std::move(descriptor), std::move(gdsAlgorithm),
        sharedState, getOperatorID(), std::move(printInfo));
    // Map node predicate pipeline
    if (call.hasNodePredicate()) {
        auto logicalRoot = call.getNodePredicateRoot();
        auto logicalSemiMasker = logicalRoot->ptrCast<LogicalSemiMasker>();
        logicalSemiMasker->addTarget(logicalOperator);
        sharedState->setPathNodeMask(
            getNodeOffsetMaskMap(logicalSemiMasker->getNodeTableIDs(), storageManager));
        logicalOpToPhysicalOpMap.insert({logicalOperator, gdsCall.get()});
        auto root = mapOperator(logicalRoot.get());
        auto dummySink = std::make_unique<DummySink>(
            std::make_unique<ResultSetDescriptor>(logicalRoot->getSchema()), std::move(root),
            getOperatorID(), std::make_unique<OPPrintInfo>());
        gdsCall->addChild(std::move(dummySink));
        logicalOpToPhysicalOpMap.erase(logicalOperator);
    }

    physical_op_vector_t children;
    children.push_back(std::move(gdsCall));
    return createFTableScanAligned(columns, outSchema, table, DEFAULT_VECTOR_CAPACITY,
        std::move(children));
}

} // namespace processor
} // namespace kuzu
