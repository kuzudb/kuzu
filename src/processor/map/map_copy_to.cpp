#include "planner/operator/persistent/logical_copy_to.h"
#include "processor/operator/persistent/copy_to_csv.h"
#include "processor/operator/persistent/copy_to_parquet.h"
#include "processor/plan_mapper.h"

using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::unique_ptr<CopyToInfo> getCopyToInfo(Schema* childSchema, ReaderConfig* config,
    std::vector<std::unique_ptr<LogicalType>> columnsTypes, std::vector<DataPos> vectorsToCopyPos,
    std::vector<bool> isFlat) {
    switch (config->fileType) {
    case FileType::PARQUET: {
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
        return std::make_unique<CopyToParquetInfo>(std::move(copyToSchema), std::move(columnsTypes),
            config->columnNames, vectorsToCopyPos, config->filePaths[0]);
    }
    case FileType::CSV: {
        return std::make_unique<CopyToCSVInfo>(
            config->columnNames, vectorsToCopyPos, config->filePaths[0], std::move(isFlat));
    }
        // LCOV_EXCL_START
    default:
        throw NotImplementedException{"getCopyToInfo"};
        // LCOV_EXCL_STOP
    }
}

std::shared_ptr<CopyToSharedState> getCopyToSharedState(ReaderConfig* config) {
    switch (config->fileType) {
    case FileType::CSV:
        return std::make_shared<CopyToCSVSharedState>();
    case FileType::PARQUET:
        return std::make_shared<CopyToParquetSharedState>();
        // LCOV_EXCL_START
    default:
        throw NotImplementedException{"getCopyToSharedState"};
        // LCOV_EXCL_STOP
    }
}

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
    std::vector<bool> isFlat;
    for (auto& expression : childSchema->getExpressionsInScope()) {
        vectorsToCopyPos.emplace_back(childSchema->getExpressionPos(*expression));
        isFlat.push_back(childSchema->getGroup(expression)->isFlat());
    }
    std::unique_ptr<CopyToInfo> copyToInfo = getCopyToInfo(childSchema, config,
        std::move(columnsTypes), std::move(vectorsToCopyPos), std::move(isFlat));
    auto sharedState = getCopyToSharedState(config);
    std::unique_ptr<CopyTo> copyTo;
    if (copy->getConfig()->fileType == common::FileType::PARQUET) {
        copyTo = std::make_unique<CopyToParquet>(std::make_unique<ResultSetDescriptor>(childSchema),
            std::move(copyToInfo), std::move(sharedState), std::move(prevOperator), getOperatorID(),
            copy->getExpressionsForPrinting());
    } else {
        copyTo = std::make_unique<CopyToCSV>(std::make_unique<ResultSetDescriptor>(childSchema),
            std::move(copyToInfo), std::move(sharedState), std::move(prevOperator), getOperatorID(),
            copy->getExpressionsForPrinting());
    }
    std::shared_ptr<FactorizedTable> fTable;
    auto ftTableSchema = std::make_unique<FactorizedTableSchema>();
    fTable = std::make_shared<FactorizedTable>(memoryManager, std::move(ftTableSchema));
    return createFactorizedTableScan(binder::expression_vector{}, std::vector<ft_col_idx_t>{},
        childSchema, fTable, 0, std::move(copyTo));
}

} // namespace processor
} // namespace kuzu
