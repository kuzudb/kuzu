#include "processor/operator/persistent/reader.h"

#include "storage/copier/npy_reader.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void Reader::initGlobalStateInternal(ExecutionContext* context) {
    sharedState->validate();
    sharedState->countBlocks();
}

void Reader::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    dataChunk = std::make_unique<DataChunk>(info->getNumColumns(),
        resultSet->getDataChunk(info->dataColumnsPos[0].dataChunkPos)->state);
    for (auto i = 0u; i < info->getNumColumns(); i++) {
        dataChunk->insert(i, resultSet->getValueVector(info->dataColumnsPos[i]));
    }
    initFunc = storage::ReaderFunctions::getInitDataFunc(sharedState->copyDescription->fileType);
    readFunc = storage::ReaderFunctions::getReadRowsFunc(sharedState->copyDescription->fileType);
    nodeOffsetVector = resultSet->getValueVector(info->nodeOffsetPos).get();
}

bool Reader::getNextTuplesInternal(ExecutionContext* context) {
    info->orderPreserving ? getNextNodeGroupInSerial() : getNextNodeGroupInParallel();
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
    while (leftArrowArrays.getLeftNumRows() < StorageConstants::NODE_GROUP_SIZE) {
        auto morsel = sharedState->getParallelMorsel();
        if (morsel->fileIdx == INVALID_VECTOR_IDX) {
            break;
        }
        if (!readFuncData || morsel->fileIdx != readFuncData->fileIdx) {
            readFuncData = initFunc(sharedState->copyDescription->filePaths, morsel->fileIdx,
                *sharedState->copyDescription->csvReaderConfig, sharedState->tableSchema);
        }
        readFunc(*readFuncData, morsel->blockIdx, dataChunk.get());
        leftArrowArrays.appendFromDataChunk(dataChunk.get());
    }
    if (leftArrowArrays.getLeftNumRows() == 0) {
        dataChunk->state->selVector->selectedSize = 0;
    } else {
        int64_t numRowsToReturn =
            std::min(leftArrowArrays.getLeftNumRows(), StorageConstants::NODE_GROUP_SIZE);
        leftArrowArrays.appendToDataChunk(dataChunk.get(), numRowsToReturn);
    }
}

} // namespace processor
} // namespace kuzu
