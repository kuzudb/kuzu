#include "processor/operator/persistent/reader/csv/csv_error_handler.h"

#include <algorithm>

#include "common/assert.h"
#include "common/exception/copy.h"
#include "common/string_format.h"
#include "processor/operator/persistent/reader/csv/base_csv_reader.h"

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

bool CSVError::operator<(const CSVError& o) const {
    if (blockIdx == o.blockIdx) {
        return numRowsReadInBlock < o.numRowsReadInBlock;
    }
    return blockIdx < o.blockIdx;
}

CSVFileErrorHandler::CSVFileErrorHandler(uint64_t maxCachedErrorCount, bool ignoreErrors)
    : maxCachedErrorCount(maxCachedErrorCount), ignoreErrors(ignoreErrors) {}

SharedCSVFileErrorHandler::SharedCSVFileErrorHandler(std::string filePath, std::mutex* sharedMtx,
    uint64_t maxCachedErrorCount, std::shared_ptr<warning_counter_t> sharedWarningCounter,
    bool ignoreErrors)
    : CSVFileErrorHandler(maxCachedErrorCount, ignoreErrors), mtx(sharedMtx),
      filePath(std::move(filePath)), headerNumRows(0),
      sharedWarningCounter(std::move(sharedWarningCounter)) {}

uint64_t SharedCSVFileErrorHandler::getNumCachedErrors() {
    return cachedErrors.size();
}

std::vector<PopulatedCSVError> SharedCSVFileErrorHandler::getPopulatedCachedErrors(
    BaseCSVReader* reader) {
    std::vector<PopulatedCSVError> errorMessages;
    for (const auto& error : cachedErrors) {
        const auto lineNumber = getLineNumber(error.blockIdx, error.numRowsReadInBlock);
        errorMessages.push_back(getPopulatedError(reader, error, lineNumber));
    }
    return errorMessages;
}

void SharedCSVFileErrorHandler::tryCacheError(const CSVError& error,
    const common::UniqLock& /*lock*/) {
    if (*sharedWarningCounter < maxCachedErrorCount) {
        cachedErrors.push_back(error);
        ++(*sharedWarningCounter);
    }
}

void SharedCSVFileErrorHandler::handleError(BaseCSVReader* reader, const CSVError& error) {
    auto lockGuard = lock();

    if (error.blockIdx >= linesPerBlock.size()) {
        linesPerBlock.resize(error.blockIdx + 1);
    }
    ++linesPerBlock[error.blockIdx].invalidLines;

    if (!error.mustThrow && (ignoreErrors || !canGetLineNumber(error.blockIdx))) {
        tryCacheError(error, lockGuard);
        return;
    }
    const auto lineNumber = getLineNumber(error.blockIdx, error.numRowsReadInBlock);
    throwError(reader, error, lineNumber);
}

void SharedCSVFileErrorHandler::throwCachedErrorsIfNeeded(BaseCSVReader* reader) {
    if (!ignoreErrors) {
        auto lockGuard = lock();
        tryThrowFirstCachedError(reader);
    }
}

void SharedCSVFileErrorHandler::tryThrowFirstCachedError(BaseCSVReader* reader) {
    if (ignoreErrors || cachedErrors.empty()) {
        return;
    }

    // we sort the cached errors to report the one with the earliest line number
    std::sort(cachedErrors.begin(), cachedErrors.end());

    const auto error = *cachedErrors.cbegin();
    KU_ASSERT(!error.mustThrow);

    const bool errorIsThrowable = canGetLineNumber(error.blockIdx);
    if (errorIsThrowable) {
        const auto lineNumber = getLineNumber(error.blockIdx, error.numRowsReadInBlock);
        throwError(reader, error, lineNumber);
    }
}

std::string SharedCSVFileErrorHandler::getErrorMessage(BaseCSVReader* reader, const CSVError& error,
    uint64_t lineNumber) const {
    const char* incompleteLineSuffix = error.errorLine.isCompleteLine ? "" : "...";
    return stringFormat("Error in file {} on line {}: {} Line containing the error: '{}{}'",
        filePath, lineNumber, error.message,
        reader->reconstructLine(error.errorLine.startByteOffset, error.errorLine.endByteOffset),
        incompleteLineSuffix);
}

PopulatedCSVError SharedCSVFileErrorHandler::getPopulatedError(BaseCSVReader* reader,
    const CSVError& error, uint64_t lineNumber) {
    const char* incompleteLineSuffix = error.errorLine.isCompleteLine ? "" : "...";
    return {.message = error.message,
        .filePath = filePath,
        .skippedLine = reader->reconstructLine(error.errorLine.startByteOffset,
                           error.errorLine.endByteOffset) +
                       incompleteLineSuffix,
        .lineNumber = lineNumber};
}

void SharedCSVFileErrorHandler::throwError(BaseCSVReader* reader, const CSVError& error,
    uint64_t lineNumber) const {
    throw CopyException(getErrorMessage(reader, error, lineNumber));
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

void SharedCSVFileErrorHandler::reportFinishedBlock(BaseCSVReader* reader, uint64_t blockIdx,
    uint64_t numRowsRead) {
    if (numRowsRead == 0) {
        return;
    }
    auto lockGuard = lock();
    if (blockIdx >= linesPerBlock.size()) {
        linesPerBlock.resize(blockIdx + 1);
    }
    linesPerBlock[blockIdx].validLines += numRowsRead;
    linesPerBlock[blockIdx].doneParsingBlock = true;

    // now that we have reported a block as finished
    // see if there are any errors we couldn't previously throw due to missing line counts
    // and see if we can throw it now
    tryThrowFirstCachedError(reader);
}

void SharedCSVFileErrorHandler::setHeaderNumRows(uint64_t numRows) {
    if (numRows == headerNumRows) {
        return;
    }
    auto lockGuard = lock();
    headerNumRows = numRows;
}

void SharedCSVFileErrorHandler::addCachedErrors(std::vector<CSVError>& errors,
    const std::map<uint64_t, LinesPerBlock>& newLinesPerBlock) {
    const auto lockGuard = lock();

    KU_ASSERT(maxCachedErrorCount >= *sharedWarningCounter);
    const auto numErrorsToAdd =
        std::min(static_cast<uint64_t>(errors.size()), maxCachedErrorCount - *sharedWarningCounter);
    for (idx_t i = 0; i < numErrorsToAdd; ++i) {
        cachedErrors.emplace_back(std::move(errors[i]));
    }
    *sharedWarningCounter += numErrorsToAdd;

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

LocalCSVFileErrorHandler::LocalCSVFileErrorHandler(uint64_t maxCachedErrorCount, bool ignoreErrors,
    SharedCSVFileErrorHandler* sharedErrorHandler)
    : CSVFileErrorHandler(std::min(maxCachedErrorCount, LOCAL_WARNING_LIMIT), ignoreErrors),
      sharedErrorHandler(sharedErrorHandler) {}

void LocalCSVFileErrorHandler::handleError(BaseCSVReader* reader, const CSVError& error) {
    if (error.mustThrow || !ignoreErrors) {
        // we delegate throwing to the shared error handler
        sharedErrorHandler->handleError(reader, error);
        return;
    }

    KU_ASSERT(cachedErrors.size() <= maxCachedErrorCount);
    if (cachedErrors.size() == maxCachedErrorCount) {
        flushCachedErrors();
    }

    ++linesPerBlock[error.blockIdx].invalidLines;
    cachedErrors.push_back(error);
}

void LocalCSVFileErrorHandler::reportFinishedBlock(BaseCSVReader* reader, uint64_t blockIdx,
    uint64_t numRowsRead) {
    sharedErrorHandler->reportFinishedBlock(reader, blockIdx, numRowsRead);
}

void LocalCSVFileErrorHandler::setHeaderNumRows(uint64_t numRows) {
    sharedErrorHandler->setHeaderNumRows(numRows);
}

LocalCSVFileErrorHandler::~LocalCSVFileErrorHandler() {
    flushCachedErrors();
}

void LocalCSVFileErrorHandler::flushCachedErrors() {
    sharedErrorHandler->addCachedErrors(cachedErrors, linesPerBlock);
    cachedErrors.clear();
    linesPerBlock.clear();
}

} // namespace processor
} // namespace kuzu
