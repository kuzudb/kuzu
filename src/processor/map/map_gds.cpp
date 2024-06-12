#include "binder/expression/node_expression.h"
#include "graph/on_disk_graph.h"
#include "planner/operator/logical_gds_call.h"
#include "processor/operator/gds_call.h"
#include "processor/plan_mapper.h"
#include "storage/storage_manager.h"

using namespace kuzu::common;
using namespace kuzu::graph;
using namespace kuzu::function;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapGDSCall(LogicalOperator* logicalOperator) {
    auto& call = logicalOperator->constCast<LogicalGDSCall>();
    auto& graphEntry = call.getInfo().graphEntry;
    if (graphEntry.nodeTableIDs.size() > 1 || graphEntry.relTableIDs.size() > 1) {
        throw RuntimeException("Using multi label graph in algorithm function is not supported.");
    }
    auto outSchema = call.getSchema();
    auto info = GDSCallInfo(call.getInfo().getGDS()->copy());
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
    auto graph = std::make_unique<OnDiskGraph>(clientContext, graphEntry.nodeTableIDs[0],
        graphEntry.relTableIDs[0]);
    std::unique_ptr<NodeOffsetSemiMask> mask = nullptr;
    if (call.getInfo().getBindData()->hasNodeInput()) {
        // Generate an empty semi mask which later on picked by SemiMaker.
        auto& node = call.getInfo().getBindData()->nodeInput->constCast<binder::NodeExpression>();
        auto tableID = node.getSingleTableID();
        auto nodeTable = clientContext->getStorageManager()->getTable(tableID);
        mask = std::make_unique<NodeOffsetSemiMask>(nodeTable->ptrCast<storage::NodeTable>());
    }
    auto sharedState =
        std::make_shared<GDSCallSharedState>(table, std::move(graph), std::move(mask));
    auto algorithm = std::make_unique<GDSCall>(std::make_unique<ResultSetDescriptor>(),
        std::move(info), sharedState, getOperatorID(), call.getExpressionsForPrinting());
    return createFTableScanAligned(columns, outSchema, table, DEFAULT_VECTOR_CAPACITY,
        std::move(algorithm));
}

} // namespace processor
} // namespace kuzu
