#include "processor/operator/table_scan/factorized_table_scan.h"
#include "processor/plan_mapper.h"

using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::createFactorizedTableScan(
    const binder::expression_vector& expressions, planner::Schema* schema,
    std::shared_ptr<FactorizedTable> table, std::unique_ptr<PhysicalOperator> prevOperator) {
    std::vector<DataPos> outputPositions;
    std::vector<ft_col_idx_t> columnIndices;
    for (auto i = 0u; i < expressions.size(); ++i) {
        outputPositions.emplace_back(schema->getExpressionPos(*expressions[i]));
        columnIndices.push_back(i);
    }
    auto info = std::make_unique<FactorizedTableScanInfo>(
        std::move(outputPositions), std::move(columnIndices));
    auto maxMorselSize = table->hasUnflatCol() ? 1 : DEFAULT_VECTOR_CAPACITY;
    auto sharedState = std::make_shared<FactorizedTableScanSharedState>(table, maxMorselSize);
    if (prevOperator == nullptr) {
        return make_unique<FactorizedTableScan>(std::move(info), sharedState, getOperatorID(),
            binder::ExpressionUtil::toString(expressions));
    }
    return make_unique<FactorizedTableScan>(std::move(info), sharedState, std::move(prevOperator),
        getOperatorID(), binder::ExpressionUtil::toString(expressions));
}

} // namespace processor
} // namespace kuzu
