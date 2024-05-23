#include "planner/operator/logical_gds_call.h"
#include "processor/operator/gds_call.h"
#include "processor/plan_mapper.h"

using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapGDSCall(LogicalOperator* logicalOperator) {
    auto call = logicalOperator->constPtrCast<LogicalGDSCall>();
    auto exprs = call->getOutExprs();
    auto outSchema = call->getSchema();
    auto info = GDSCallInfo(call->getFunction().gds->copy(), call->getGraphExpr());
    auto tableSchema = std::make_unique<FactorizedTableSchema>();
    for (auto& e : exprs) {
        auto dataPos = getDataPos(*e, *outSchema);
        auto columnSchema = ColumnSchema(false /* isUnFlat */, dataPos.dataChunkPos,
            LogicalTypeUtils::getRowLayoutSize(e->getDataType()));
        tableSchema->appendColumn(std::move(columnSchema));
    }
    auto table =
        std::make_shared<FactorizedTable>(clientContext->getMemoryManager(), tableSchema->copy());
    auto sharedState = std::make_shared<GDSCallSharedState>(table);
    auto algorithm = std::make_unique<GDSCall>(std::make_unique<ResultSetDescriptor>(),
        std::move(info), sharedState, getOperatorID(), call->getExpressionsForPrinting());
    return createFTableScanAligned(call->getOutExprs(), outSchema, table, DEFAULT_VECTOR_CAPACITY,
        std::move(algorithm));
}

} // namespace processor
} // namespace kuzu
