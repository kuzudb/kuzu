#include "processor/operator/persistent/reader_state.h"

#include <optional>

using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

void ReaderSharedState::initialize(MemoryManager* memoryManager, TableType tableType) {
    validateFunc = ReaderFunctions::getValidateFunc(readerConfig->fileType);
    initFunc = ReaderFunctions::getInitDataFunc(*readerConfig);
    countRowsFunc = ReaderFunctions::getCountRowsFunc(*readerConfig, tableType);
    readFuncData = ReaderFunctions::getReadFuncData(*readerConfig);

    // Initialize for readers that share readFuncData.
    if (readerConfig->fileType == FileType::CSV && !readerConfig->csvReaderConfig->parallel) {
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
