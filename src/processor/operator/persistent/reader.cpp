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
    dataChunkToRead = std::make_unique<DataChunk>(readerInfo.dataColumnPoses.size(),
        resultSet->getDataChunk(readerInfo.dataColumnPoses[0].dataChunkPos)->state);
    for (auto i = 0u; i < readerInfo.dataColumnPoses.size(); i++) {
        dataChunkToRead->insert(i, resultSet->getValueVector(readerInfo.dataColumnPoses[i]));
    }
}

bool Reader::getNextTuplesInternal(ExecutionContext* context) {
    readerInfo.isOrderPreserving ? getNextNodeGroupInSerial() : getNextNodeGroupInParallel();
    return dataChunkToRead->state->selVector->selectedSize != 0;
}

void Reader::getNextNodeGroupInSerial() {
    auto morsel = sharedState->getSerialMorsel(dataChunkToRead.get());
    if (morsel->fileIdx == INVALID_VECTOR_IDX) {
        return;
    }
    auto nodeOffsetVector = resultSet->getValueVector(readerInfo.nodeOffsetPos).get();
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
            readFuncData = readerInfo.initFunc(sharedState->filePaths, morsel->fileIdx,
                sharedState->csvReaderConfig, sharedState->tableSchema);
        }
        readerInfo.readFunc(*readFuncData, morsel->blockIdx, dataChunkToRead.get());
        leftArrowArrays.appendFromDataChunk(dataChunkToRead.get());
    }
    if (leftArrowArrays.getLeftNumRows() == 0) {
        dataChunkToRead->state->selVector->selectedSize = 0;
    } else {
        int64_t numRowsToReturn =
            std::min(leftArrowArrays.getLeftNumRows(), StorageConstants::NODE_GROUP_SIZE);
        leftArrowArrays.appendToDataChunk(dataChunkToRead.get(), numRowsToReturn);
    }
}

} // namespace processor
} // namespace kuzu
