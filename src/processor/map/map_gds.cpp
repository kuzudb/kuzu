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
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapGDSCall(LogicalOperator* logicalOperator) {
    auto& call = logicalOperator->constCast<LogicalGDSCall>();
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
    auto graph = std::make_unique<OnDiskGraph>(clientContext, call.getInfo().graphEntry);
    common::table_id_map_t<std::unique_ptr<NodeOffsetLevelSemiMask>> masks;
    if (call.getInfo().getBindData()->hasNodeInput()) {
        // Generate an empty semi mask which later on picked by SemiMaker.
        auto& node =
            call.getInfo().getBindData()->getNodeInput()->constCast<binder::NodeExpression>();
        for (auto tableID : node.getTableIDs()) {
            auto nodeTable =
                clientContext->getStorageManager()->getTable(tableID)->ptrCast<NodeTable>();
            masks.insert({tableID, std::make_unique<NodeOffsetLevelSemiMask>(tableID,
                                       nodeTable->getMaxNodeOffset(clientContext->getTx()))});
        }
    }
    auto sharedState =
        std::make_shared<GDSCallSharedState>(table, std::move(graph), std::move(masks));
    auto printInfo = std::make_unique<OPPrintInfo>(call.getExpressionsForPrinting());
    auto gdsCall = std::make_unique<GDSCall>(std::make_unique<ResultSetDescriptor>(),
        std::move(info), sharedState, getOperatorID(), std::move(printInfo));
    return createFTableScanAligned(columns, outSchema, table, DEFAULT_VECTOR_CAPACITY,
        std::move(gdsCall));
}

} // namespace processor
} // namespace kuzu
