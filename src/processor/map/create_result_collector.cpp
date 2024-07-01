#include "processor/operator/result_collector.h"
#include "processor/plan_mapper.h"
#include "processor/result/factorized_table_util.h"

using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::binder;

namespace kuzu {
namespace processor {

std::unique_ptr<ResultCollector> PlanMapper::createResultCollector(AccumulateType accumulateType,
    const expression_vector& expressions, Schema* schema,
    std::unique_ptr<PhysicalOperator> prevOperator) {
    std::vector<DataPos> payloadsPos;
    for (auto& expr : expressions) {
        payloadsPos.push_back(getDataPos(*expr, *schema));
    }
    auto tableSchema = FactorizedTableUtils::createFTableSchema(expressions, *schema);
    if (accumulateType == AccumulateType::OPTIONAL_) {
        auto columnSchema = ColumnSchema(false /* isUnFlat */, INVALID_DATA_CHUNK_POS,
            LogicalTypeUtils::getRowLayoutSize(LogicalType::BOOL()));
        tableSchema.appendColumn(std::move(columnSchema));
    }
    auto table =
        std::make_shared<FactorizedTable>(clientContext->getMemoryManager(), tableSchema.copy());
    auto sharedState = std::make_shared<ResultCollectorSharedState>(std::move(table));
    auto opInfo = ResultCollectorInfo(accumulateType, std::move(tableSchema), payloadsPos);
    auto printInfo = std::make_unique<ResultCollectorPrintInfo>(expressions, accumulateType);
    return make_unique<ResultCollector>(std::make_unique<ResultSetDescriptor>(schema),
        std::move(opInfo), std::move(sharedState), std::move(prevOperator), getOperatorID(),
        std::move(printInfo));
}

} // namespace processor
} // namespace kuzu
