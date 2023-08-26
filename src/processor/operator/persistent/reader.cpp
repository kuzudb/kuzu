#include "processor/operator/persistent/reader.h"

#include "storage/copier/npy_reader.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

bool Reader::getNextTuplesInternal(ExecutionContext* context) {
    std::shared_ptr<arrow::Table> table = nullptr;
    readerInfo.isOrderPreserving ? getNextNodeGroupInSerial(table) :
                                   getNextNodeGroupInParallel(table);
    if (table == nullptr) {
        return false;
    }
    for (auto i = 0u; i < readerInfo.dataColumnPoses.size(); i++) {
        ArrowColumnVector::setArrowColumn(
            resultSet->getValueVector(readerInfo.dataColumnPoses[i]).get(), table->column((int)i));
    }
    return true;
}

void Reader::getNextNodeGroupInSerial(std::shared_ptr<arrow::Table>& table) {
    auto morsel = sharedState->getSerialMorsel();
    if (morsel->fileIdx == INVALID_VECTOR_IDX) {
        return;
    }
    auto serialMorsel = reinterpret_cast<SerialReaderMorsel*>(morsel.get());
    table = serialMorsel->table;
    auto nodeOffsetVector = resultSet->getValueVector(readerInfo.nodeOffsetPos).get();
    nodeOffsetVector->setValue(
        nodeOffsetVector->state->selVector->selectedPositions[0], morsel->rowIdx);
}

void Reader::getNextNodeGroupInParallel(std::shared_ptr<arrow::Table>& table) {
    while (leftNumRows < StorageConstants::NODE_GROUP_SIZE) {
        auto morsel = sharedState->getParallelMorsel();
        if (morsel->fileIdx == INVALID_VECTOR_IDX) {
            break;
        }
        if (!readFuncData || morsel->fileIdx != readFuncData->fileIdx) {
            readFuncData = readerInfo.initFunc(sharedState->filePaths, morsel->fileIdx,
                sharedState->csvReaderConfig, sharedState->tableSchema);
        }
        auto batchVector = readerInfo.readFunc(*readFuncData, morsel->blockIdx);
        for (auto& batch : batchVector) {
            leftNumRows += batch->num_rows();
            leftRecordBatches.push_back(std::move(batch));
        }
    }
    if (leftNumRows == 0) {
        return;
    }
    table = ReaderSharedState::constructTableFromBatches(leftRecordBatches);
    leftNumRows -= table->num_rows();
}

} // namespace processor
} // namespace kuzu
