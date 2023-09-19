#include "processor/operator/persistent/reader.h"

#include "processor/operator/persistent/reader/npy_reader.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void Reader::initGlobalStateInternal(ExecutionContext* context) {
    sharedState->initialize(info->tableType);
    sharedState->validate();
    sharedState->countBlocks(context->memoryManager);
}

void Reader::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    memoryManager = context->memoryManager;
    dataChunk = std::make_unique<DataChunk>(info->getNumColumns(),
        resultSet->getDataChunk(info->dataColumnsPos[0].dataChunkPos)->state);
    for (auto i = 0u; i < info->getNumColumns(); i++) {
        dataChunk->insert(i, resultSet->getValueVector(info->dataColumnsPos[i]));
    }
    initFunc =
        ReaderFunctions::getInitDataFunc(sharedState->readerConfig->fileType, info->tableType);
    readFunc =
        ReaderFunctions::getReadRowsFunc(sharedState->readerConfig->fileType, info->tableType);
    if (info->nodeOffsetPos.dataChunkPos != INVALID_DATA_CHUNK_POS) {
        offsetVector = resultSet->getValueVector(info->nodeOffsetPos).get();
    }
    assert(!sharedState->readerConfig->filePaths.empty());
    switch (sharedState->readerConfig->fileType) {
    case FileType::CSV: {
        readFuncData = sharedState->readFuncData;
    } break;
    default: {
        readFuncData =
            ReaderFunctions::getReadFuncData(sharedState->readerConfig->fileType, info->tableType);
        initFunc(*readFuncData, 0, *sharedState->readerConfig, context->memoryManager);
    }
    }
}

bool Reader::getNextTuplesInternal(ExecutionContext* context) {
    sharedState->readerConfig->parallelRead() ?
        readNextDataChunk<ReaderSharedState::ReadMode::PARALLEL>() :
        readNextDataChunk<ReaderSharedState::ReadMode::SERIAL>();
    return dataChunk->state->selVector->selectedSize != 0;
}

template<ReaderSharedState::ReadMode READ_MODE>
void Reader::readNextDataChunk() {
    lockForSerial<READ_MODE>();
    while (true) {
        if (leftArrowArrays.getLeftNumRows() > 0) {
            auto numLeftToAppend =
                std::min(leftArrowArrays.getLeftNumRows(), DEFAULT_VECTOR_CAPACITY);
            leftArrowArrays.appendToDataChunk(dataChunk.get(), numLeftToAppend);
            auto currRowIdx = sharedState->currRowIdx.fetch_add(numLeftToAppend);
            if (offsetVector != nullptr) {
                offsetVector->setValue(
                    offsetVector->state->selVector->selectedPositions[0], currRowIdx);
            }
            break;
        }
        dataChunk->state->selVector->selectedSize = 0;
        dataChunk->resetAuxiliaryBuffer();
        if (readFuncData->hasMoreToRead()) {
            readFunc(*readFuncData, UINT64_MAX /* blockIdx */, dataChunk.get());
            if (dataChunk->state->selVector->selectedSize > 0) {
                leftArrowArrays.appendFromDataChunk(dataChunk.get());
                continue;
            }
        }
        auto morsel = sharedState->getMorsel<READ_MODE>();
        if (morsel->fileIdx == INVALID_VECTOR_IDX) {
            // No more files to read.
            break;
        }
        if (morsel->fileIdx != readFuncData->fileIdx) {
            initFunc(*readFuncData, morsel->fileIdx, *sharedState->readerConfig, memoryManager);
        }
        readFunc(*readFuncData, morsel->blockIdx, dataChunk.get());
        if (dataChunk->state->selVector->selectedSize > 0) {
            leftArrowArrays.appendFromDataChunk(dataChunk.get());
        } else {
            sharedState->moveToNextFile();
        }
    }
    unlockForSerial<READ_MODE>();
}

template void Reader::readNextDataChunk<ReaderSharedState::ReadMode::SERIAL>();
template void Reader::readNextDataChunk<ReaderSharedState::ReadMode::PARALLEL>();

} // namespace processor
} // namespace kuzu
