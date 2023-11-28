#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "binder/expression/expression.h"
#include "binder/expression/expression_util.h"
#include "common/enums/join_type.h"
#include "common/types/types.h"
#include "planner/operator/schema.h"
#include "processor/data_pos.h"
#include "processor/operator/physical_operator.h"
#include "processor/operator/result_collector.h"
#include "processor/plan_mapper.h"
#include "processor/result/factorized_table.h"
#include "processor/result/result_set_descriptor.h"

using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<ResultCollector> PlanMapper::createResultCollector(AccumulateType accumulateType,
    const binder::expression_vector& expressions, Schema* schema,
    std::unique_ptr<PhysicalOperator> prevOperator) {
    std::vector<DataPos> payloadsPos;
    auto tableSchema = std::make_unique<FactorizedTableSchema>();
    for (auto& expression : expressions) {
        auto dataPos = DataPos(schema->getExpressionPos(*expression));
        std::unique_ptr<ColumnSchema> columnSchema;
        if (schema->getGroup(dataPos.dataChunkPos)->isFlat()) {
            columnSchema = std::make_unique<ColumnSchema>(false /* isUnFlat */,
                dataPos.dataChunkPos, LogicalTypeUtils::getRowLayoutSize(expression->dataType));
        } else {
            columnSchema = std::make_unique<ColumnSchema>(
                true /* isUnFlat */, dataPos.dataChunkPos, (uint32_t)sizeof(overflow_value_t));
        }
        tableSchema->appendColumn(std::move(columnSchema));
        payloadsPos.push_back(dataPos);
    }
    auto table = std::make_shared<FactorizedTable>(memoryManager, tableSchema->copy());
    auto sharedState = std::make_shared<ResultCollectorSharedState>(std::move(table));
    auto info =
        std::make_unique<ResultCollectorInfo>(accumulateType, std::move(tableSchema), payloadsPos);
    return make_unique<ResultCollector>(std::make_unique<ResultSetDescriptor>(schema),
        std::move(info), std::move(sharedState), std::move(prevOperator), getOperatorID(),
        binder::ExpressionUtil::toString(expressions));
}

} // namespace processor
} // namespace kuzu
