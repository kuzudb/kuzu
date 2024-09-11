#include "processor/operator/persistent/reader/csv/csv_error_handler.h"

#include <algorithm>

#include "common/assert.h"
#include "common/exception/copy.h"
#include "common/string_format.h"
#include "processor/operator/persistent/reader/csv/base_csv_reader.h"
#include "processor/operator/persistent/reader/csv/serial_csv_reader.h"

namespace kuzu {
using namespace common;
namespace processor {

void LineContext::setNewLine(uint64_t start) {
    startByteOffset = start;
    isCompleteLine = false;
}

void LineContext::setEndOfLine(uint64_t end) {
    endByteOffset = end;
    isCompleteLine = true;
}

SharedCSVFileErrorHandler::SharedCSVFileErrorHandler(std::string filePath, std::mutex* sharedMtx)
    : mtx(sharedMtx), filePath(std::move(filePath)), headerNumRows(0) {}

uint64_t SharedCSVFileErrorHandler::getNumCachedErrors() {
    return cachedErrors.size();
}

populate_func_t SharedCSVFileErrorHandler::getPopulateCSVErrorFunc() {
    return [this](CSVError error, SerialCSVReader* reader) -> PopulatedCSVError {
        const auto lineNumber =
            getLineNumber(error.warningData.blockIdx, error.warningData.rowOffsetInBlock);
        return getPopulatedError(reader, std::move(error), lineNumber);
    };
}

void SharedCSVFileErrorHandler::tryCacheError(CSVError error, const common::UniqLock&) {
    if (cachedErrors.size() < MAX_CACHED_ERROR_COUNT) {
        cachedErrors.push_back(std::move(error));
    }
}

void SharedCSVFileErrorHandler::handleError(BaseCSVReader* reader, CSVError error) {
    auto lockGuard = lock();

    const auto blockIdx = error.warningData.blockIdx;
    if (blockIdx >= linesPerBlock.size()) {
        linesPerBlock.resize(blockIdx + 1);
    }
    ++linesPerBlock[blockIdx].invalidLines;

    if (error.mustThrow || canGetLineNumber(blockIdx)) {
        const auto lineNumber = getLineNumber(blockIdx, error.warningData.rowOffsetInBlock);
        throwError(reader, std::move(error), lineNumber);
    } else {
        // we only cache an error in the shared handler if we cannot throw right now but we want to
        // do it later
        // for the sake of reporting warnings, errors are sent directly to
        // WarningContext
        tryCacheError(std::move(error), lockGuard);
    }
}

void SharedCSVFileErrorHandler::throwCachedErrorsIfNeeded(BaseCSVReader* reader) {
    auto lockGuard = lock();
    tryThrowFirstCachedError(reader);
}

void SharedCSVFileErrorHandler::tryThrowFirstCachedError(BaseCSVReader* reader) {
    if (cachedErrors.empty()) {
        return;
    }

    // we sort the cached errors to report the one with the earliest line number
    std::sort(cachedErrors.begin(), cachedErrors.end());

    const auto error = *cachedErrors.cbegin();
    KU_ASSERT(!error.mustThrow);

    const bool errorIsThrowable = canGetLineNumber(error.warningData.blockIdx);
    if (errorIsThrowable) {
        const auto lineNumber =
            getLineNumber(error.warningData.blockIdx, error.warningData.rowOffsetInBlock);
        throwError(reader, error, lineNumber);
    }
}

std::string SharedCSVFileErrorHandler::getErrorMessage(BaseCSVReader* reader, CSVError error,
    uint64_t lineNumber) const {
    const auto populatedError = getPopulatedError(reader, std::move(error), lineNumber);
    return stringFormat("Error in file {} on line {}: {} Line containing the error: '{}'",
        populatedError.filePath, lineNumber, populatedError.message, populatedError.skippedLine);
}

PopulatedCSVError SharedCSVFileErrorHandler::getPopulatedError(BaseCSVReader* reader,
    CSVError error, uint64_t lineNumber) const {
    const char* incompleteLineSuffix = error.completedLine ? "" : "...";
    return {.message = std::move(error.message),
        .filePath = filePath,
        .skippedLine = reader->reconstructLine(error.warningData.startByteOffset,
                           error.warningData.endByteOffset) +
                       incompleteLineSuffix,
        .lineNumber = lineNumber};
}

void SharedCSVFileErrorHandler::throwError(BaseCSVReader* reader, CSVError error,
    uint64_t lineNumber) const {
    throw CopyException(getErrorMessage(reader, std::move(error), lineNumber));
}

common::UniqLock SharedCSVFileErrorHandler::lock() {
    if (mtx) {
        return common::UniqLock{*mtx};
    }
    return common::UniqLock{};
}

bool SharedCSVFileErrorHandler::canGetLineNumber(uint64_t blockIdx) const {
    if (blockIdx > linesPerBlock.size()) {
        return false;
    }
    for (uint64_t i = 0; i < blockIdx; ++i) {
        //  the line count for a block is empty if it hasn't finished being parsed
        if (!linesPerBlock[i].doneParsingBlock) {
            return false;
        }
    }
    return true;
}

uint64_t SharedCSVFileErrorHandler::getLineNumber(uint64_t blockIdx,
    uint64_t numRowsReadInBlock) const {
    // 1-indexed
    uint64_t res = numRowsReadInBlock + headerNumRows + 1;
    for (uint64_t i = 0; i < blockIdx; ++i) {
        KU_ASSERT(i < linesPerBlock.size());
        res += linesPerBlock[i].validLines + linesPerBlock[i].invalidLines;
    }
    return res;
}

void SharedCSVFileErrorHandler::setHeaderNumRows(uint64_t numRows) {
    if (numRows == headerNumRows) {
        return;
    }
    auto lockGuard = lock();
    headerNumRows = numRows;
}

void SharedCSVFileErrorHandler::updateLineNumberInfo(
    const std::map<uint64_t, LinesPerBlock>& newLinesPerBlock) {
    const auto lockGuard = lock();

    if (!newLinesPerBlock.empty()) {
        const auto maxNewBlockIdx = newLinesPerBlock.rbegin()->first;
        if (maxNewBlockIdx >= linesPerBlock.size()) {
            linesPerBlock.resize(maxNewBlockIdx + 1);
        }

        for (const auto& [blockIdx, linesInBlock] : newLinesPerBlock) {
            auto& currentBlock = linesPerBlock[blockIdx];
            currentBlock.validLines += linesInBlock.validLines;
            currentBlock.invalidLines += linesInBlock.invalidLines;
            currentBlock.doneParsingBlock =
                currentBlock.doneParsingBlock || linesInBlock.doneParsingBlock;
        }
    }
}

LocalCSVFileErrorHandler::LocalCSVFileErrorHandler(SharedCSVFileErrorHandler* sharedErrorHandler,
    bool ignoreErrors, main::ClientContext* context, bool cacheErrors)
    : sharedErrorHandler(sharedErrorHandler), context(context),
      maxCachedErrorCount(
          std::min(this->context->getClientConfig()->warningLimit, LOCAL_WARNING_LIMIT)),
      ignoreErrors(ignoreErrors), cacheIgnoredErrors(cacheErrors) {}

void LocalCSVFileErrorHandler::handleError(BaseCSVReader* reader, CSVError error) {
    if (error.mustThrow || !ignoreErrors) {
        // we delegate throwing to the shared error handler
        sharedErrorHandler->handleError(reader, std::move(error));
        return;
    }

    KU_ASSERT(cachedErrors.size() <= maxCachedErrorCount);
    if (cachedErrors.size() == maxCachedErrorCount) {
        flushCachedErrors();
    }

    ++linesPerBlock[error.warningData.blockIdx].invalidLines;
    if (cacheIgnoredErrors) {
        cachedErrors.push_back(std::move(error));
    }
}

void LocalCSVFileErrorHandler::reportFinishedBlock(uint64_t blockIdx, uint64_t numRowsRead) {
    linesPerBlock[blockIdx].validLines += numRowsRead;
    linesPerBlock[blockIdx].doneParsingBlock = true;
}

void LocalCSVFileErrorHandler::setHeaderNumRows(uint64_t numRows) {
    sharedErrorHandler->setHeaderNumRows(numRows);
}

LocalCSVFileErrorHandler::~LocalCSVFileErrorHandler() {
    finalize();
}

void LocalCSVFileErrorHandler::finalize() {
    flushCachedErrors();
}

void LocalCSVFileErrorHandler::flushCachedErrors() {
    if (!linesPerBlock.empty()) {
        sharedErrorHandler->updateLineNumberInfo(linesPerBlock);
        linesPerBlock.clear();
    }

    if (!cachedErrors.empty()) {
        context->getWarningContextUnsafe().appendWarningMessages(cachedErrors);
        cachedErrors.clear();
    }
}

} // namespace processor
} // namespace kuzu
