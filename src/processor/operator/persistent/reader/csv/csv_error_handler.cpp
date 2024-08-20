#include "processor/operator/persistent/reader/csv/csv_error_handler.h"

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

CSVFileErrorHandler::CSVFileErrorHandler(std::mutex* sharedMtx, uint64_t maxCachedErrorCount,
    std::shared_ptr<warning_counter_t> sharedWarningCounter, bool ignoreErrors)
    : mtx(sharedMtx), maxCachedErrorCount(maxCachedErrorCount), ignoreErrors(ignoreErrors),
      headerNumRows(0), sharedWarningCounter(std::move(sharedWarningCounter)) {}

uint64_t CSVFileErrorHandler::getNumCachedErrors() {
    return cachedErrors.size();
}

std::vector<PopulatedCSVError> CSVFileErrorHandler::getPopulatedCachedErrors(
    BaseCSVReader* reader) {
    std::vector<PopulatedCSVError> errorMessages;
    for (const auto& error : cachedErrors) {
        const auto lineNumber = getLineNumber(error.blockIdx, error.numRowsReadInBlock);
        errorMessages.push_back(getPopulatedError(reader, error, lineNumber));
    }
    return errorMessages;
}

void CSVFileErrorHandler::tryCacheError(const CSVError& error, const common::UniqLock& /*lock*/) {
    if (*sharedWarningCounter < maxCachedErrorCount) {
        cachedErrors.insert(error);
        ++(*sharedWarningCounter);
    }
}

void CSVFileErrorHandler::handleError(BaseCSVReader* reader, const CSVError& error) {
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

void CSVFileErrorHandler::throwCachedErrorsIfNeeded(BaseCSVReader* reader) {
    if (!ignoreErrors) {
        auto lockGuard = lock();
        tryThrowFirstCachedError(reader);
    }
}

void CSVFileErrorHandler::tryThrowFirstCachedError(BaseCSVReader* reader) const {
    if (ignoreErrors || cachedErrors.empty()) {
        return;
    }
    const auto error = *cachedErrors.cbegin();
    KU_ASSERT(!error.mustThrow);

    const bool errorIsThrowable = canGetLineNumber(error.blockIdx);
    if (errorIsThrowable) {
        const auto lineNumber = getLineNumber(error.blockIdx, error.numRowsReadInBlock);
        throwError(reader, error, lineNumber);
    }
}

std::string CSVFileErrorHandler::getErrorMessage(BaseCSVReader* reader, const CSVError& error,
    uint64_t lineNumber) const {
    const char* incompleteLineSuffix = error.errorLine.isCompleteLine ? "" : "...";
    return stringFormat("Error in file {} on line {}: {} Line containing the error: '{}{}'",
        error.filePath, lineNumber, error.message,
        reader->reconstructLine(error.errorLine.startByteOffset, error.errorLine.endByteOffset),
        incompleteLineSuffix);
}

PopulatedCSVError CSVFileErrorHandler::getPopulatedError(BaseCSVReader* reader,
    const CSVError& error, uint64_t lineNumber) {
    const char* incompleteLineSuffix = error.errorLine.isCompleteLine ? "" : "...";
    return {.message = error.message,
        .filePath = error.filePath,
        .reconstructedLine = reader->reconstructLine(error.errorLine.startByteOffset,
                                 error.errorLine.endByteOffset) +
                             incompleteLineSuffix,
        .lineNumber = lineNumber};
}

void CSVFileErrorHandler::throwError(BaseCSVReader* reader, const CSVError& error,
    uint64_t lineNumber) const {
    throw CopyException(getErrorMessage(reader, error, lineNumber));
}

common::UniqLock CSVFileErrorHandler::lock() {
    if (mtx) {
        return common::UniqLock{*mtx};
    }
    return common::UniqLock{};
}

bool CSVFileErrorHandler::canGetLineNumber(uint64_t blockIdx) const {
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

uint64_t CSVFileErrorHandler::getLineNumber(uint64_t blockIdx, uint64_t numRowsReadInBlock) const {
    // 1-indexed
    uint64_t res = numRowsReadInBlock + headerNumRows + 1;
    for (uint64_t i = 0; i < blockIdx; ++i) {
        KU_ASSERT(i < linesPerBlock.size());
        res += linesPerBlock[i].validLines + linesPerBlock[i].invalidLines;
    }
    return res;
}

void CSVFileErrorHandler::reportFinishedBlock(BaseCSVReader* reader, uint64_t blockIdx,
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

void CSVFileErrorHandler::setHeaderNumRows(uint64_t numRows) {
    if (numRows == 0) {
        return;
    }
    auto lockGuard = lock();
    headerNumRows = numRows;
}
} // namespace processor
} // namespace kuzu
