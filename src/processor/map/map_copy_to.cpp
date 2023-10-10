#include "planner/operator/persistent/logical_copy_to.h"
#include "processor/operator/persistent/copy_to.h"
#include "processor/operator/persistent/copy_to_parquet.h"
#include "processor/plan_mapper.h"

using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapCopyTo(LogicalOperator* logicalOperator) {
    auto copy = (LogicalCopyTo*)logicalOperator;
    auto config = copy->getConfig();
    std::vector<std::unique_ptr<LogicalType>> columnsTypes;
    std::vector<std::string> columnNames;
    columnsTypes.reserve(config->columnTypes.size());
    for (auto& type : config->columnTypes) {
        columnsTypes.push_back(type->copy());
    }
    auto childSchema = logicalOperator->getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    std::vector<DataPos> vectorsToCopyPos;
    for (auto& expression : childSchema->getExpressionsInScope()) {
        vectorsToCopyPos.emplace_back(childSchema->getExpressionPos(*expression));
    }
    if (copy->getConfig()->fileType == common::FileType::PARQUET) {
        auto copyToSchema = std::make_unique<FactorizedTableSchema>();
        auto copyToExpressions = childSchema->getExpressionsInScope();
        for (auto& copyToExpression : copyToExpressions) {
            auto [dataChunkPos, vectorPos] = childSchema->getExpressionPos(*copyToExpression);
            std::unique_ptr<ColumnSchema> columnSchema;
            if (!childSchema->getGroup(dataChunkPos)->isFlat()) {
                // payload is unFlat and not in the same group as keys
                columnSchema = std::make_unique<ColumnSchema>(
                    true /* isUnFlat */, dataChunkPos, sizeof(overflow_value_t));
            } else {
                columnSchema = std::make_unique<ColumnSchema>(false /* isUnFlat */, dataChunkPos,
                    LogicalTypeUtils::getRowLayoutSize(copyToExpression->getDataType()));
            }
            copyToSchema->appendColumn(std::move(columnSchema));
        }

        auto copyToParquetInfo = std::make_unique<CopyToParquetInfo>(std::move(copyToSchema),
            std::move(columnsTypes), config->columnNames, vectorsToCopyPos, config->filePaths[0]);
        auto copyTo =
            std::make_unique<CopyToParquet>(std::make_unique<ResultSetDescriptor>(childSchema),
                std::move(copyToParquetInfo), std::make_shared<CopyToParquetSharedState>(),
                std::move(prevOperator), getOperatorID(), copy->getExpressionsForPrinting());
        std::shared_ptr<FactorizedTable> fTable;
        auto ftTableSchema = std::make_unique<FactorizedTableSchema>();
        fTable = std::make_shared<FactorizedTable>(memoryManager, std::move(ftTableSchema));
        return createFactorizedTableScan(binder::expression_vector{}, std::vector<ft_col_idx_t>{},
            childSchema, fTable, 0, std::move(copyTo));
    } else {
        auto sharedState = std::make_shared<CopyToSharedState>(
            config->fileType, config->filePaths[0], config->columnNames, std::move(columnsTypes));
        auto copyTo = std::make_unique<CopyTo>(std::make_unique<ResultSetDescriptor>(childSchema),
            sharedState, std::move(vectorsToCopyPos), getOperatorID(),
            copy->getExpressionsForPrinting(), std::move(prevOperator));
        std::shared_ptr<FactorizedTable> fTable;
        auto ftTableSchema = std::make_unique<FactorizedTableSchema>();
        fTable = std::make_shared<FactorizedTable>(memoryManager, std::move(ftTableSchema));
        return createFactorizedTableScan(binder::expression_vector{}, std::vector<ft_col_idx_t>{},
            childSchema, fTable, 0, std::move(copyTo));
    }
}

} // namespace processor
} // namespace kuzu
