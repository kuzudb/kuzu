#include "processor/execution_context.h"

#include "processor/operator/persistent/reader/csv/csv_error.h"
#include "processor/result/factorized_table_util.h"

namespace kuzu {
using namespace common;
namespace processor {

ExecutionContext::ExecutionContext(common::Profiler* profiler, main::ClientContext* clientContext,
    uint64_t queryID)
    : queryID{queryID}, profiler{profiler}, clientContext{clientContext} {
    FactorizedTableSchema tableSchema;

    // message
    tableSchema.appendColumn(
        ColumnSchema{false, 0, LogicalTypeUtils::getRowLayoutSize(LogicalType::STRING())});

    // file path
    tableSchema.appendColumn(
        ColumnSchema{false, 1, LogicalTypeUtils::getRowLayoutSize(LogicalType::STRING())});

    // line number
    tableSchema.appendColumn(
        ColumnSchema{false, 2, LogicalTypeUtils::getRowLayoutSize(LogicalType::UINT64())});

    // reconstructed line
    tableSchema.appendColumn(
        ColumnSchema{false, 3, LogicalTypeUtils::getRowLayoutSize(LogicalType::STRING())});

    warningTable = std::make_unique<FactorizedTable>(this->clientContext->getMemoryManager(),
        std::move(tableSchema));
}

// marking this function with const would be misleading here
// NOLINTNEXTLINE(readability-make-member-function-const)
void ExecutionContext::setWarningMessages(const std::vector<PopulatedCSVError>& messages) {
    common::UniqLock lock{mtx};
    for (const auto& error : messages) {
        auto lineVec =
            std::make_shared<ValueVector>(LogicalType::UINT64(), clientContext->getMemoryManager());
        lineVec->setValue(0, error.lineNumber);
        lineVec->state = DataChunkState::getSingleValueDataChunkState();

        auto messageVec = FactorizedTableUtils::getVectorToAppendFromString(error.message,
            clientContext->getMemoryManager());
        auto pathVec = FactorizedTableUtils::getVectorToAppendFromString(error.filePath,
            clientContext->getMemoryManager());
        auto reconstructedLineVec = FactorizedTableUtils::getVectorToAppendFromString(
            error.reconstructedLine, clientContext->getMemoryManager());

        std::vector<ValueVector*> vecs{
            {messageVec.get(), pathVec.get(), lineVec.get(), reconstructedLineVec.get()}};
        warningTable->append(vecs);
    }
}

std::shared_ptr<FactorizedTable>& ExecutionContext::getWarningTable() {
    return warningTable;
}

} // namespace processor
} // namespace kuzu
