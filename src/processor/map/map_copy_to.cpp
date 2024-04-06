#include "planner/operator/persistent/logical_copy_to.h"
#include "processor/operator/persistent/copy_to_csv.h"
#include "processor/operator/persistent/copy_to_parquet.h"
#include "processor/plan_mapper.h"

using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::unique_ptr<CopyToInfo> getCopyToInfo(Schema* childSchema, std::string filePath,
    common::FileType fileType, common::CSVOption copyToOption, std::vector<std::string> columnNames,
    std::vector<std::unique_ptr<LogicalType>> columnsTypes, std::vector<DataPos> vectorsToCopyPos,
    std::vector<bool> isFlat) {
    switch (fileType) {
    case FileType::PARQUET: {
        auto copyToSchema = std::make_unique<FactorizedTableSchema>();
        auto copyToExpressions = childSchema->getExpressionsInScope();
        auto countingVecPos = DataPos(childSchema->getExpressionPos(*copyToExpressions[0]));
        for (auto& copyToExpression : copyToExpressions) {
            auto [dataChunkPos, vectorPos] = childSchema->getExpressionPos(*copyToExpression);
            std::unique_ptr<ColumnSchema> columnSchema;
            if (!childSchema->getGroup(dataChunkPos)->isFlat()) {
                // payload is unFlat and not in the same group as keys
                columnSchema = std::make_unique<ColumnSchema>(true /* isUnFlat */, dataChunkPos,
                    sizeof(overflow_value_t));
                countingVecPos = DataPos(childSchema->getExpressionPos(*copyToExpression));
            } else {
                columnSchema = std::make_unique<ColumnSchema>(false /* isUnFlat */, dataChunkPos,
                    LogicalTypeUtils::getRowLayoutSize(copyToExpression->getDataType()));
            }
            copyToSchema->appendColumn(std::move(columnSchema));
        }
        return std::make_unique<CopyToParquetInfo>(std::move(copyToSchema), std::move(columnsTypes),
            std::move(columnNames), std::move(vectorsToCopyPos), std::move(filePath),
            std::move(countingVecPos));
    }
    case FileType::CSV: {
        return std::make_unique<CopyToCSVInfo>(std::move(columnNames), std::move(vectorsToCopyPos),
            std::move(filePath), std::move(isFlat), std::move(copyToOption));
    }
    default:
        KU_UNREACHABLE;
    }
}

static std::shared_ptr<CopyToSharedState> getCopyToSharedState(FileType fileType) {
    switch (fileType) {
    case FileType::CSV:
        return std::make_shared<CopyToCSVSharedState>();
    case FileType::PARQUET:
        return std::make_shared<CopyToParquetSharedState>();
    default:
        KU_UNREACHABLE;
    }
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapCopyTo(LogicalOperator* logicalOperator) {
    auto copy = (LogicalCopyTo*)logicalOperator;
    auto columnNames = copy->getColumnNames();
    std::vector<std::unique_ptr<LogicalType>> columnTypes;
    for (auto& type : copy->getColumnTypesRef()) {
        columnTypes.push_back(std::make_unique<LogicalType>(type));
    }
    auto childSchema = logicalOperator->getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    std::vector<DataPos> vectorsToCopyPos;
    std::vector<bool> isFlat;
    for (auto& expression : childSchema->getExpressionsInScope()) {
        vectorsToCopyPos.emplace_back(childSchema->getExpressionPos(*expression));
        isFlat.push_back(childSchema->getGroup(expression)->isFlat());
    }
    std::unique_ptr<CopyToInfo> copyToInfo = getCopyToInfo(childSchema, copy->getFilePath(),
        copy->getFileType(), copy->getCopyOption()->copy(), std::move(columnNames),
        std::move(columnTypes), std::move(vectorsToCopyPos), std::move(isFlat));
    auto sharedState = getCopyToSharedState(copy->getFileType());
    std::unique_ptr<CopyTo> copyTo;
    if (copy->getFileType() == common::FileType::PARQUET) {
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
    fTable = std::make_shared<FactorizedTable>(clientContext->getMemoryManager(),
        std::move(ftTableSchema));
    return createEmptyFTableScan(fTable, 0, std::move(copyTo));
}

} // namespace processor
} // namespace kuzu
