#include "processor/operator/copy/read_npy.h"

#include "common/constants.h"
#include "storage/in_mem_storage_structure/in_mem_column_chunk.h"

using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::shared_ptr<arrow::RecordBatch> ReadNPY::readTuples(std::unique_ptr<ReadFileMorsel> morsel) {
    assert(!morsel->filePath.empty());
    if (!reader || reader->getFilePath() != morsel->filePath) {
        reader = std::make_unique<storage::NpyReader>(morsel->filePath);
    }
    auto batch = reader->readBlock(morsel->blockIdx);
    return batch;
}

bool ReadNPY::getNextTuplesInternal(kuzu::processor::ExecutionContext* context) {
    auto sharedStateNPY = reinterpret_cast<ReadNPYSharedState*>(sharedState.get());
    auto morsel = sharedStateNPY->getMorsel();
    if (morsel == nullptr) {
        return false;
    }
    auto npyMorsel = reinterpret_cast<ReadNPYMorsel*>(morsel.get());
    auto startRowIdx = npyMorsel->rowIdx;
    auto columnIdx = npyMorsel->getColumnIdx();
    rowIdxVector->setValue(rowIdxVector->state->selVector->selectedPositions[0], startRowIdx);
    columnIdxVector->setValue(columnIdxVector->state->selVector->selectedPositions[0], columnIdx);
    auto recordBatch = readTuples(std::move(morsel));
    common::ArrowColumnVector::setArrowColumn(
        arrowColumnVectors[columnIdx], recordBatch->column((int)0));
    return true;
}

void ReadNPY::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    ReadFile::initLocalStateInternal(resultSet, context);
    columnIdxVector = resultSet->getValueVector(columnIdxPos).get();
}

} // namespace processor
} // namespace kuzu
