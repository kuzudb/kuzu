#include "processor/operator/persistent/reader_state.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::storage;

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

void ReaderSharedState::initialize(TableType tableType) {
    validateFunc = ReaderFunctions::getValidateFunc(readerConfig->fileType);
    initFunc = ReaderFunctions::getInitDataFunc(readerConfig->fileType, tableType);
    countBlocksFunc = ReaderFunctions::getCountBlocksFunc(readerConfig->fileType, tableType);
    readFunc = ReaderFunctions::getReadRowsFunc(readerConfig->fileType, tableType);
    readFuncData = ReaderFunctions::getReadFuncData(readerConfig->fileType, tableType);
}

void ReaderSharedState::validate() const {
    validateFunc(*readerConfig);
}

void ReaderSharedState::countBlocks(MemoryManager* memoryManager) {
    initFunc(*readFuncData, 0 /* fileIdx */, *readerConfig, memoryManager);
    fileInfos = countBlocksFunc(*readerConfig, memoryManager);
    for (auto& fileInfo : fileInfos) {
        numRows += fileInfo.numRows;
    }
}

template<ReaderSharedState::ReadMode READ_MODE>
std::unique_ptr<ReaderMorsel> ReaderSharedState::getMorsel() {
    std::unique_ptr<ReaderMorsel> morsel;
    lockForParallel<READ_MODE>();
    while (true) {
        if (currFileIdx >= readerConfig->getNumFiles()) {
            // No more blocks.
            morsel = std::make_unique<ReaderMorsel>();
            break;
        }
        if (currBlockIdx < fileInfos[currFileIdx].numBlocks) {
            morsel = std::make_unique<ReaderMorsel>(currFileIdx, currBlockIdx++);
            break;
        }
        moveToNextFile();
    }
    unlockForParallel<READ_MODE>();
    return morsel;
}

template std::unique_ptr<ReaderMorsel>
ReaderSharedState::getMorsel<ReaderSharedState::ReadMode::SERIAL>();
template std::unique_ptr<ReaderMorsel>
ReaderSharedState::getMorsel<ReaderSharedState::ReadMode::PARALLEL>();

} // namespace processor
} // namespace kuzu
