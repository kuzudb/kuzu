#include "binder/expression/expression_util.h"
#include "processor/operator/result_collector.h"
#include "processor/plan_mapper.h"
#include "processor/result/factorized_table_util.h"

using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::binder;

namespace kuzu {
namespace processor {

std::unique_ptr<ResultCollector> PlanMapper::createResultCollector(AccumulateType accumulateType,
    const binder::expression_vector& expressions, Schema* schema,
    std::unique_ptr<PhysicalOperator> prevOperator) {
    std::vector<DataPos> payloadsPos;
    for (auto& e : expressions) {
        payloadsPos.push_back(getDataPos(*e, *schema));
    }
    auto tableSchema = FactorizedTableUtils::createFTableSchema(expressions, *schema);
    if (accumulateType == AccumulateType::OPTIONAL_) {
        auto columnSchema = ColumnSchema(false /* isUnFlat */, INVALID_DATA_CHUNK_POS,
            LogicalTypeUtils::getRowLayoutSize(*LogicalType::BOOL()));
        tableSchema.appendColumn(std::move(columnSchema));
    }
    auto table =
        std::make_shared<FactorizedTable>(clientContext->getMemoryManager(), tableSchema.copy());
    auto sharedState = std::make_shared<ResultCollectorSharedState>(std::move(table));
    auto info =
        std::make_unique<ResultCollectorInfo>(accumulateType, std::move(tableSchema), payloadsPos);
    return make_unique<ResultCollector>(std::make_unique<ResultSetDescriptor>(schema),
        std::move(info), std::move(sharedState), std::move(prevOperator), getOperatorID(),
        binder::ExpressionUtil::toString(expressions));
}

} // namespace processor
} // namespace kuzu
