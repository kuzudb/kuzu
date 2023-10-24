#include "processor/operator/persistent/reader_state.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::storage;

#include "arrow/array.h"

namespace kuzu {
namespace processor {

void LeftArrowArrays::appendFromDataChunk(common::DataChunk* dataChunk) {
    leftNumRows += dataChunk->state->selVector->selectedSize;
    if (dataChunk->getValueVector(0)->dataType.getPhysicalType() == PhysicalTypeID::ARROW_COLUMN) {
        leftArrays.resize(dataChunk->getNumValueVectors());
        for (auto i = 0u; i < dataChunk->getNumValueVectors(); i++) {
            for (auto& array :
                ArrowColumnVector::getArrowColumn(dataChunk->getValueVector(i).get())->chunks()) {
                leftArrays[i].push_back(array);
            }
        }
    }
}

void LeftArrowArrays::appendToDataChunk(common::DataChunk* dataChunk, uint64_t numRowsToAppend) {
    leftNumRows -= numRowsToAppend;
    if (dataChunk->getValueVector(0)->dataType.getPhysicalType() != PhysicalTypeID::ARROW_COLUMN) {
        return;
    }
    int64_t numRowsAppended = 0;
    auto& arrayVectorToComputeSlice = leftArrays[0];
    std::vector<arrow::ArrayVector> arrayVectors;
    arrayVectors.resize(leftArrays.size());
    uint64_t arrayVectorIdx;
    for (arrayVectorIdx = 0u; arrayVectorIdx < arrayVectorToComputeSlice.size(); arrayVectorIdx++) {
        if (numRowsAppended >= numRowsToAppend) {
            break;
        } else {
            auto arrayToComputeSlice = arrayVectorToComputeSlice[arrayVectorIdx];
            int64_t numRowsToAppendInCurArray = arrayToComputeSlice->length();
            if (numRowsToAppend - numRowsAppended < arrayToComputeSlice->length()) {
                numRowsToAppendInCurArray = (int64_t)numRowsToAppend - numRowsAppended;
                for (auto j = 0u; j < leftArrays.size(); j++) {
                    auto vectorToSlice = leftArrays[j][arrayVectorIdx];
                    leftArrays[j].push_back(vectorToSlice->Slice(numRowsToAppendInCurArray));
                    arrayVectors[j].push_back(vectorToSlice->Slice(0, numRowsToAppendInCurArray));
                }
            } else {
                for (auto j = 0u; j < leftArrays.size(); j++) {
                    arrayVectors[j].push_back(leftArrays[j][arrayVectorIdx]);
                }
            }
            numRowsAppended += numRowsToAppendInCurArray;
        }
    }
    for (auto& arrayVector : leftArrays) {
        arrayVector.erase(arrayVector.begin(), arrayVector.begin() + arrayVectorIdx);
    }
    for (auto i = 0u; i < dataChunk->getNumValueVectors(); i++) {
        auto chunkArray = std::make_shared<arrow::ChunkedArray>(std::move(arrayVectors[i]));
        ArrowColumnVector::setArrowColumn(
            dataChunk->getValueVector(i).get(), std::move(chunkArray));
    }
    dataChunk->state->selVector->selectedSize = numRowsToAppend;
}

void ReaderSharedState::initialize(MemoryManager* memoryManager, TableType tableType) {
    validateFunc = ReaderFunctions::getValidateFunc(readerConfig->fileType);
    initFunc = ReaderFunctions::getInitDataFunc(*readerConfig, tableType);
    countRowsFunc = ReaderFunctions::getCountRowsFunc(*readerConfig, tableType);
    readFuncData = ReaderFunctions::getReadFuncData(*readerConfig, tableType);

    // Initialize for readers that share readFuncData.
    if (readerConfig->fileType == FileType::CSV && !readerConfig->csvParallelRead(tableType)) {
        initFunc(*readFuncData, 0 /* fileIdx */, *readerConfig, memoryManager);
    }
}

void ReaderSharedState::validate() const {
    validateFunc(*readerConfig);
}

void ReaderSharedState::countRows(MemoryManager* memoryManager) {
    numRows = countRowsFunc(*readerConfig, memoryManager);
}

template<ReaderSharedState::ReadMode READ_MODE>
[[nodiscard]] static std::optional<std::lock_guard<std::mutex>> lockIfParallel(std::mutex& mtx) {
    if constexpr (READ_MODE == ReaderSharedState::ReadMode::PARALLEL) {
        return std::make_optional<std::lock_guard<std::mutex>>(mtx);
    } else {
        return std::nullopt;
    }
}

template<ReaderSharedState::ReadMode READ_MODE>
void ReaderSharedState::doneFile(vector_idx_t fileIdx) {
    auto lckGuard = lockIfParallel<READ_MODE>(mtx);
    if (fileIdx == currFileIdx) {
        currFileIdx +=
            (readerConfig->fileType == common::FileType::NPY ? readerConfig->filePaths.size() : 1);
        currBlockIdx = 0;
    }
}

template void ReaderSharedState::doneFile<ReaderSharedState::ReadMode::SERIAL>(
    vector_idx_t fileIdx);
template void ReaderSharedState::doneFile<ReaderSharedState::ReadMode::PARALLEL>(
    vector_idx_t fileIdx);

template<ReaderSharedState::ReadMode READ_MODE>
std::unique_ptr<ReaderMorsel> ReaderSharedState::getMorsel() {
    std::unique_ptr<ReaderMorsel> morsel;
    auto lckGuard = lockIfParallel<READ_MODE>(mtx);
    if (currFileIdx >= readerConfig->getNumFiles()) {
        // No more blocks.
        morsel = std::make_unique<ReaderMorsel>();
    } else {
        morsel = std::make_unique<ReaderMorsel>(currFileIdx, currBlockIdx++);
    }
    return morsel;
}

template std::unique_ptr<ReaderMorsel>
ReaderSharedState::getMorsel<ReaderSharedState::ReadMode::SERIAL>();
template std::unique_ptr<ReaderMorsel>
ReaderSharedState::getMorsel<ReaderSharedState::ReadMode::PARALLEL>();

} // namespace processor
} // namespace kuzu
