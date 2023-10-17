#include "processor/operator/persistent/reader.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void Reader::initGlobalStateInternal(ExecutionContext* context) {
    sharedState->initialize(context->memoryManager, info->tableType);
    sharedState->validate();
    sharedState->countRows(context->memoryManager);
}

void Reader::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    memoryManager = context->memoryManager;
    dataChunk = std::make_unique<DataChunk>(info->getNumColumns(),
        resultSet->getDataChunk(info->dataColumnsPos[0].dataChunkPos)->state);
    for (auto i = 0u; i < info->getNumColumns(); i++) {
        dataChunk->insert(i, resultSet->getValueVector(info->dataColumnsPos[i]));
    }
    initFunc = ReaderFunctions::getInitDataFunc(*sharedState->readerConfig, info->tableType);
    readFunc = ReaderFunctions::getReadRowsFunc(*sharedState->readerConfig, info->tableType);
    if (info->internalIDPos.dataChunkPos != INVALID_DATA_CHUNK_POS) {
        internalIDVector = resultSet->getValueVector(info->internalIDPos).get();
    }
    assert(!sharedState->readerConfig->filePaths.empty());
    if (sharedState->readerConfig->fileType == FileType::CSV &&
        !sharedState->readerConfig->csvParallelRead(info->tableType)) {
        readFuncData = sharedState->readFuncData;
    } else {
        readFuncData =
            ReaderFunctions::getReadFuncData(*sharedState->readerConfig, info->tableType);
        initFunc(*readFuncData, 0, *sharedState->readerConfig, memoryManager);
    }
}

bool Reader::getNextTuplesInternal(ExecutionContext* /*context*/) {
    sharedState->readerConfig->parallelRead(info->tableType) ?
        readNextDataChunk<ReaderSharedState::ReadMode::PARALLEL>() :
        readNextDataChunk<ReaderSharedState::ReadMode::SERIAL>();
    return dataChunk->state->selVector->selectedSize != 0;
}

template<ReaderSharedState::ReadMode READ_MODE>
void Reader::readNextDataChunk() {
    auto lckGuard = lockIfSerial<READ_MODE>();
    while (true) {
        if (leftArrowArrays.getLeftNumRows() > 0) {
            auto numLeftToAppend =
                std::min(leftArrowArrays.getLeftNumRows(), DEFAULT_VECTOR_CAPACITY);
            leftArrowArrays.appendToDataChunk(dataChunk.get(), numLeftToAppend);
            auto currRowIdx = sharedState->currRowIdx.fetch_add(numLeftToAppend);
            if (internalIDVector != nullptr) {
                internalIDVector
                    ->getValue<internalID_t>(
                        internalIDVector->state->selVector->selectedPositions[0])
                    .offset = currRowIdx;
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
        } else if (readFuncData->doneAfterEmptyBlock()) {
            sharedState->doneFile<READ_MODE>(morsel->fileIdx);
        }
    }
}

template void Reader::readNextDataChunk<ReaderSharedState::ReadMode::SERIAL>();
template void Reader::readNextDataChunk<ReaderSharedState::ReadMode::PARALLEL>();

} // namespace processor
} // namespace kuzu
