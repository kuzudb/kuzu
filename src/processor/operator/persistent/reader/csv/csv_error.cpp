#include "processor/operator/persistent/reader/csv/csv_error.h"

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

CSVErrorHandler::CSVErrorHandler(std::mutex* sharedMtx, uint64_t maxCachedErrorCount,
    warning_counter_t* sharedWarningCounter, bool ignoreErrors)
    : mtx(sharedMtx), maxCachedErrorCount(maxCachedErrorCount), ignoreErrors(ignoreErrors),
      headerNumRows(0), sharedWarningCounter(sharedWarningCounter) {}

uint64_t CSVErrorHandler::getCachedErrorCount() {
    return cachedErrors.size();
}

std::vector<PopulatedCSVError> CSVErrorHandler::getCachedErrors(BaseCSVReader* reader) {
    std::vector<PopulatedCSVError> errorMessages;
    for (const auto& error : cachedErrors) {
        const auto lineNumber = getLineNumber(error.blockIdx, error.numRowsReadInBlock);
        errorMessages.push_back(getPopulatedError(reader, error, lineNumber));
    }
    return errorMessages;
}

void CSVErrorHandler::tryCacheError(const CSVError& error) {
    if (*sharedWarningCounter < maxCachedErrorCount) {
        cachedErrors.insert(error);
        ++(*sharedWarningCounter);
    }
}

void CSVErrorHandler::handleError(BaseCSVReader* reader, const CSVError& error, bool mustThrow) {
    auto lockGuard = lock();

    if (error.blockIdx >= linesPerBlock.size()) {
        linesPerBlock.resize(error.blockIdx + 1);
    }
    ++linesPerBlock[error.blockIdx].invalidLines;

    if (!mustThrow && (ignoreErrors || !canGetLineNumber(error.blockIdx))) {
        tryCacheError(error);
        return;
    }
    const auto lineNumber = getLineNumber(error.blockIdx, error.numRowsReadInBlock);
    throwError(reader, error, lineNumber);
}

void CSVErrorHandler::handleCachedErrors(BaseCSVReader* reader) {
    if (!ignoreErrors) {
        auto lockGuard = lock();
        tryThrowFirstCachedError(reader, true);
    }
}

void CSVErrorHandler::tryThrowFirstCachedError(BaseCSVReader* reader,
    [[maybe_unused]] bool mustThrow) const {
    if (ignoreErrors || cachedErrors.empty()) {
        return;
    }
    const auto error = *cachedErrors.cbegin();

    const bool errorIsThrowable = canGetLineNumber(error.blockIdx);
    if (errorIsThrowable) {
        const auto lineNumber = getLineNumber(error.blockIdx, error.numRowsReadInBlock);
        throwError(reader, error, lineNumber);
    } else if (mustThrow) {
        throwError(reader, error, 0);
    }
}

std::string CSVErrorHandler::getErrorMessage(BaseCSVReader* reader, const CSVError& error,
    uint64_t lineNumber) const {
    const char* incompleteLineSuffix = error.errorLine.isCompleteLine ? "" : "...";
    return stringFormat("Error in file {} on line {}: {} Line containing the error: '{}{}'",
        error.filePath, lineNumber, error.message,
        reader->reconstructLine(error.errorLine.startByteOffset, error.errorLine.endByteOffset),
        incompleteLineSuffix);
}

PopulatedCSVError CSVErrorHandler::getPopulatedError(BaseCSVReader* reader, const CSVError& error,
    uint64_t lineNumber) {
    const char* incompleteLineSuffix = error.errorLine.isCompleteLine ? "" : "...";
    return {.message = error.message,
        .filePath = error.filePath,
        .reconstructedLine = reader->reconstructLine(error.errorLine.startByteOffset,
                                 error.errorLine.endByteOffset) +
                             incompleteLineSuffix,
        .lineNumber = lineNumber};
}

void CSVErrorHandler::throwError(BaseCSVReader* reader, const CSVError& error,
    uint64_t lineNumber) const {
    throw CopyException(getErrorMessage(reader, error, lineNumber));
}

common::UniqLock CSVErrorHandler::lock() {
    if (mtx) {
        return common::UniqLock{*mtx};
    }
    return common::UniqLock{};
}

bool CSVErrorHandler::canGetLineNumber(uint64_t blockIdx) const {
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

uint64_t CSVErrorHandler::getLineNumber(uint64_t blockIdx, uint64_t numRowsReadInBlock) const {
    // 1-indexed
    uint64_t res = numRowsReadInBlock + headerNumRows + 1;
    for (uint64_t i = 0; i < blockIdx; ++i) {
        KU_ASSERT(i < linesPerBlock.size());
        res += linesPerBlock[i].validLines + linesPerBlock[i].invalidLines;
    }
    return res;
}

void CSVErrorHandler::reportFinishedBlock(BaseCSVReader* reader, uint64_t blockIdx,
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

void CSVErrorHandler::setHeaderNumRows(uint64_t numRows) {
    if (numRows == 0) {
        return;
    }
    auto lockGuard = lock();
    headerNumRows = numRows;
}
} // namespace processor
} // namespace kuzu
