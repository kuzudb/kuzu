#include "binder/expression/node_expression.h"
#include "function/gds/parallel_utils.h"
#include "graph/in_mem_graph.h"
#include "graph/on_disk_graph.h"
#include "planner/operator/logical_gds_call.h"
#include "processor/operator/csr_index_build.h"
#include "processor/operator/flatten.h"
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
    auto columns = call.getInfo().outExpressions;
    auto srcInternalIDExpr =
        call.getInfo().srcNodeIDExpression->constPtrCast<binder::NodeExpression>()->getInternalID();
    for (auto& expr : columns) {
        auto dataPos = getDataPos(*expr, *outSchema);
        if (expr->getUniqueName() == srcInternalIDExpr->getUniqueName()) {
            auto columnSchema = ColumnSchema(false /* isUnFlat */, dataPos.dataChunkPos,
                LogicalTypeUtils::getRowLayoutSize(expr->getDataType()));
            tableSchema->appendColumn(std::move(columnSchema));
        } else {
            auto columnSchema = ColumnSchema(true /* isUnFlat */, dataPos.dataChunkPos,
                (uint32_t)sizeof(overflow_value_t));
            tableSchema->appendColumn(std::move(columnSchema));
        }
    }
    auto table =
        std::make_shared<FactorizedTable>(clientContext->getMemoryManager(), tableSchema->copy());
    auto& graphEntry = call.getInfo().graphEntry;
    common::table_id_map_t<std::unique_ptr<NodeOffsetLevelSemiMask>> masks;
    if (call.getInfo().getBindData()->hasNodeInput()) {
        // Generate an empty semi mask which later on picked by SemiMaker.
        auto& node = call.getInfo().getBindData()->nodeInput->constCast<binder::NodeExpression>();
        for (auto tableID : node.getTableIDs()) {
            auto nodeTable =
                clientContext->getStorageManager()->getTable(tableID)->ptrCast<NodeTable>();
            masks.insert({tableID, std::make_unique<NodeOffsetLevelSemiMask>(tableID,
                                       nodeTable->getMaxNodeOffset(clientContext->getTx()))});
        }
    }
    auto parallelUtils =
        std::make_shared<ParallelUtils>(getOperatorID(), clientContext->getTaskScheduler());
    info.gds->setParallelUtils(parallelUtils);
    if (graphEntry.inMemProjection) {
        auto child = mapOperator(logicalOperator->getChild(0).get());
        KU_ASSERT(child->getOperatorType() == PhysicalOperatorType::CSR_INDEX_BUILD);
        auto csrIndexBuild = ku_dynamic_cast<PhysicalOperator*, CSRIndexBuild*>(child.get());
        auto graph = std::make_unique<InMemGraph>(clientContext, graphEntry.nodeTableIDs[0],
                graphEntry.relTableIDs[0], csrIndexBuild->getCSRSharedState());
        auto sharedState =
            std::make_shared<GDSCallSharedState>(table, std::move(graph), std::move(masks));
        auto printInfo = std::make_unique<OPPrintInfo>(call.getExpressionsForPrinting());
        auto algorithm = std::make_unique<GDSCall>(std::make_unique<ResultSetDescriptor>(),
            std::move(info), sharedState, std::move(child), getOperatorID(), std::move(printInfo));
        auto ftableScan =
            createFTableScanAligned(columns, outSchema, table, 1u, std::move(algorithm));
        binder::Expression& expr = *srcInternalIDExpr.get();
        auto flattenPrintInfo = std::make_unique<OPPrintInfo>(expr.getUniqueName());
        auto flattensOperator = make_unique<Flatten>(outSchema->getGroupPos(expr),
            std::move(ftableScan), getOperatorID(), std::move(flattenPrintInfo));
        return flattensOperator;
    } else {
        auto graph = std::make_unique<OnDiskGraph>(clientContext, graphEntry.nodeTableIDs[0],
            graphEntry.relTableIDs[0]);
        auto sharedState =
            std::make_shared<GDSCallSharedState>(table, std::move(graph), std::move(masks));
        auto printInfo = std::make_unique<OPPrintInfo>(call.getExpressionsForPrinting());
        auto algorithm = std::make_unique<GDSCall>(std::make_unique<ResultSetDescriptor>(),
            std::move(info), sharedState, getOperatorID(), std::move(printInfo));
        auto ftableScan =
            createFTableScanAligned(columns, outSchema, table, 1u, std::move(algorithm));
        binder::Expression& expr = *srcInternalIDExpr.get();
        auto flattenPrintInfo = std::make_unique<OPPrintInfo>(expr.getUniqueName());
        auto flattensOperator = make_unique<Flatten>(outSchema->getGroupPos(expr),
            std::move(ftableScan), getOperatorID(), std::move(flattenPrintInfo));
        return flattensOperator;
    }
}

} // namespace processor
} // namespace kuzu
