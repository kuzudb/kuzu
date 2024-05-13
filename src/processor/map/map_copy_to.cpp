#include "planner/operator/persistent/logical_copy_to.h"
#include "processor/operator/persistent/copy_to_csv.h"
#include "processor/operator/persistent/copy_to_parquet.h"
#include "processor/plan_mapper.h"
#include "processor/result/factorized_table_util.h"

using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::unique_ptr<CopyToInfo> getCopyToInfo(Schema* childSchema, std::string filePath,
    common::FileType fileType, common::CSVOption copyToOption, std::vector<std::string> columnNames,
    std::vector<std::unique_ptr<LogicalType>> columnsTypes, std::vector<DataPos> vectorsToCopyPos,
    std::vector<bool> isFlat, bool canParallel) {
    switch (fileType) {
    case FileType::PARQUET: {
        auto expressions = childSchema->getExpressionsInScope();
        auto tableSchema = FactorizedTableUtils::createFTableSchema(expressions, *childSchema);
        auto countingVecPos = DataPos(childSchema->getExpressionPos(*expressions[0]));
        for (auto& e : expressions) {
            auto group = childSchema->getGroup(e->getUniqueName());
            if (!group->isFlat()) {
                countingVecPos = DataPos(childSchema->getExpressionPos(*e));
            }
        }

        return std::make_unique<CopyToParquetInfo>(std::move(tableSchema), std::move(columnsTypes),
            std::move(columnNames), std::move(vectorsToCopyPos), std::move(filePath),
            std::move(countingVecPos), canParallel);
    }
    case FileType::CSV: {
        return std::make_unique<CopyToCSVInfo>(std::move(columnNames), std::move(vectorsToCopyPos),
            std::move(filePath), std::move(isFlat), std::move(copyToOption), canParallel);
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
    std::unique_ptr<CopyToInfo> copyToInfo =
        getCopyToInfo(childSchema, copy->getFilePath(), copy->getFileType(),
            copy->getCopyOption()->copy(), std::move(columnNames), std::move(columnTypes),
            std::move(vectorsToCopyPos), std::move(isFlat), copy->getCanParallel());
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
    auto fTable = std::make_shared<FactorizedTable>(clientContext->getMemoryManager(),
        FactorizedTableSchema());
    return createEmptyFTableScan(fTable, 0, std::move(copyTo));
}

} // namespace processor
} // namespace kuzu
