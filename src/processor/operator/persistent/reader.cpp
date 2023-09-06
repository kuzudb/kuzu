#include "processor/operator/persistent/reader.h"

#include "storage/copier/npy_reader.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void Reader::initGlobalStateInternal(ExecutionContext* context) {
    sharedState->validate();
    sharedState->countBlocks(context->memoryManager);
}

void Reader::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    dataChunk = std::make_unique<DataChunk>(info->getNumColumns(),
        resultSet->getDataChunk(info->dataColumnsPos[0].dataChunkPos)->state);
    for (auto i = 0u; i < info->getNumColumns(); i++) {
        dataChunk->insert(i, resultSet->getValueVector(info->dataColumnsPos[i]));
    }
    initFunc = ReaderFunctions::getInitDataFunc(
        sharedState->copyDescription->fileType, sharedState->tableSchema->getTableType());
    readFunc = ReaderFunctions::getReadRowsFunc(
        sharedState->copyDescription->fileType, sharedState->tableSchema->getTableType());
    nodeOffsetVector = resultSet->getValueVector(info->nodeOffsetPos).get();
}

bool Reader::getNextTuplesInternal(ExecutionContext* context) {
    sharedState->copyDescription->fileType == common::CopyDescription::FileType::CSV ?
        getNextNodeGroupInSerial() :
        getNextNodeGroupInParallel();
    return dataChunk->state->selVector->selectedSize != 0;
}

void Reader::getNextNodeGroupInSerial() {
    auto morsel = sharedState->getSerialMorsel(dataChunk.get());
    if (morsel->fileIdx == INVALID_VECTOR_IDX) {
        return;
    }
    nodeOffsetVector->setValue(
        nodeOffsetVector->state->selVector->selectedPositions[0], morsel->rowIdx);
}

void Reader::getNextNodeGroupInParallel() {
    readNextNodeGroupInParallel();
    if (leftArrowArrays.getLeftNumRows() == 0) {
        dataChunk->state->selVector->selectedSize = 0;
    } else {
        int64_t numRowsToReturn =
            std::min(leftArrowArrays.getLeftNumRows(), DEFAULT_VECTOR_CAPACITY);
        leftArrowArrays.appendToDataChunk(dataChunk.get(), numRowsToReturn);
    }
}

void Reader::readNextNodeGroupInParallel() {
    if (leftArrowArrays.getLeftNumRows() == 0) {
        auto morsel = sharedState->getParallelMorsel();
        if (morsel->fileIdx == INVALID_VECTOR_IDX) {
            return;
        }
        if (!readFuncData || morsel->fileIdx != readFuncData->fileIdx) {
            readFuncData = initFunc(sharedState->copyDescription->filePaths, morsel->fileIdx,
                *sharedState->copyDescription->csvReaderConfig, sharedState->tableSchema);
        }
        readFunc(*readFuncData, morsel->blockIdx, dataChunk.get());
        leftArrowArrays.appendFromDataChunk(dataChunk.get());
    }
}

} // namespace processor
} // namespace kuzu
