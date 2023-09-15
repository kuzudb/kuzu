#include "planner/operator/persistent/logical_copy_to.h"
#include "processor/operator/persistent/copy_to.h"
#include "processor/plan_mapper.h"

using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapCopyTo(LogicalOperator* logicalOperator) {
    auto copy = (LogicalCopyTo*)logicalOperator;
    auto config = copy->getConfig();
    std::vector<LogicalType> columnsTypes;
    for (auto& type : config->columnTypes) {
        columnsTypes.push_back(*type);
    }
    auto childSchema = logicalOperator->getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    std::vector<DataPos> vectorsToCopyPos;
    for (auto& expression : childSchema->getExpressionsInScope()) {
        vectorsToCopyPos.emplace_back(childSchema->getExpressionPos(*expression));
    }
    auto sharedState = std::make_shared<CopyToSharedState>(
        config->fileType, config->filePaths[0], config->columnNames, columnsTypes);
    auto copyTo = std::make_unique<CopyTo>(std::make_unique<ResultSetDescriptor>(childSchema),
        sharedState, std::move(vectorsToCopyPos), getOperatorID(),
        copy->getExpressionsForPrinting(), std::move(prevOperator));
    std::shared_ptr<FactorizedTable> fTable;
    auto ftTableSchema = std::make_unique<FactorizedTableSchema>();
    fTable = std::make_shared<FactorizedTable>(memoryManager, std::move(ftTableSchema));
    return createFactorizedTableScan(binder::expression_vector{}, std::vector<ft_col_idx_t>{},
        childSchema, fTable, 0, std::move(copyTo));
}

} // namespace processor
} // namespace kuzu
