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
    initFunc = ReaderFunctions::getInitDataFunc(*sharedState->readerConfig);
    readFunc = ReaderFunctions::getReadRowsFunc(*sharedState->readerConfig);
    if (info->offsetPos.dataChunkPos != INVALID_DATA_CHUNK_POS) {
        offsetVector = resultSet->getValueVector(info->offsetPos).get();
        KU_ASSERT(offsetVector->dataType.getPhysicalType() == PhysicalTypeID::INT64);
    }
    KU_ASSERT(!sharedState->readerConfig->filePaths.empty());
    if (sharedState->readerConfig->fileType == FileType::CSV &&
        !sharedState->readerConfig->csvReaderConfig->parallel) {
        readFuncData = sharedState->readFuncData;
    } else {
        readFuncData = ReaderFunctions::getReadFuncData(*sharedState->readerConfig);
        initFunc(*readFuncData, 0, *sharedState->readerConfig, memoryManager);
    }
}

bool Reader::getNextTuplesInternal(ExecutionContext* /*context*/) {
    sharedState->readerConfig->parallelRead() ?
        readNextDataChunk<ReaderSharedState::ReadMode::PARALLEL>() :
        readNextDataChunk<ReaderSharedState::ReadMode::SERIAL>();
    return dataChunk->state->selVector->selectedSize != 0;
}

template<ReaderSharedState::ReadMode READ_MODE>
void Reader::readNextDataChunk() {
    auto lckGuard = lockIfSerial<READ_MODE>();
    while (true) {
        if (leftNumRows > 0) {
            auto numLeftToAppend = std::min(leftNumRows, DEFAULT_VECTOR_CAPACITY);
            leftNumRows -= numLeftToAppend;
            auto currRowIdx = sharedState->currRowIdx.fetch_add(numLeftToAppend);
            if (offsetVector != nullptr) {
                // TODO(Guodong): Fill rel offsets. Move to separate function.
                auto offsets = ((offset_t*)offsetVector->getData());
                for (auto i = 0u; i < numLeftToAppend; i++) {
                    offsets[offsetVector->state->selVector->selectedPositions[i]] = currRowIdx + i;
                }
            }
            break;
        }
        dataChunk->state->selVector->selectedSize = 0;
        dataChunk->resetAuxiliaryBuffer();
        if (readFuncData->hasMoreToRead()) {
            readFunc(*readFuncData, UINT64_MAX /* blockIdx */, dataChunk.get());
            if (dataChunk->state->selVector->selectedSize > 0) {
                leftNumRows += dataChunk->state->selVector->selectedSize;
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
            leftNumRows += dataChunk->state->selVector->selectedSize;
        } else if (readFuncData->doneAfterEmptyBlock()) {
            sharedState->doneFile<READ_MODE>(morsel->fileIdx);
        }
    }
}

template void Reader::readNextDataChunk<ReaderSharedState::ReadMode::SERIAL>();
template void Reader::readNextDataChunk<ReaderSharedState::ReadMode::PARALLEL>();

} // namespace processor
} // namespace kuzu
