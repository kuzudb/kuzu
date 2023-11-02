#include "binder/expression/expression_util.h"
#include "processor/operator/table_scan/factorized_table_scan.h"
#include "processor/plan_mapper.h"

using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::binder;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::createFactorizedTableScan(
    const expression_vector& expressions, std::vector<ft_col_idx_t> colIndices, Schema* schema,
    std::shared_ptr<FactorizedTable> table, uint64_t maxMorselSize,
    std::unique_ptr<PhysicalOperator> prevOperator) {
    std::vector<DataPos> outputPositions;
    for (auto i = 0u; i < expressions.size(); ++i) {
        outputPositions.emplace_back(schema->getExpressionPos(*expressions[i]));
    }
    auto info = std::make_unique<FactorizedTableScanInfo>(
        std::move(outputPositions), std::move(colIndices));
    auto sharedState = std::make_shared<FactorizedTableScanSharedState>(table, maxMorselSize);
    if (prevOperator == nullptr) {
        return make_unique<FactorizedTableScan>(std::move(info), sharedState, getOperatorID(),
            binder::ExpressionUtil::toString(expressions));
    }
    return make_unique<FactorizedTableScan>(std::move(info), sharedState, std::move(prevOperator),
        getOperatorID(), binder::ExpressionUtil::toString(expressions));
}

std::unique_ptr<PhysicalOperator> PlanMapper::createFactorizedTableScanAligned(
    const expression_vector& expressions, Schema* schema, std::shared_ptr<FactorizedTable> table,
    uint64_t maxMorselSize, std::unique_ptr<PhysicalOperator> prevOperator) {
    std::vector<ft_col_idx_t> columnIndices;
    for (auto i = 0u; i < expressions.size(); ++i) {
        columnIndices.push_back(i);
    }
    return createFactorizedTableScan(expressions, std::move(columnIndices), schema, table,
        maxMorselSize, std::move(prevOperator));
}

} // namespace processor
} // namespace kuzu
