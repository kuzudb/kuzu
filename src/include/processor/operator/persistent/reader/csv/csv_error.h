#pragma once

#include <set>
#include <string>
#include <vector>

#include "common/uniq_lock.h"

namespace kuzu::processor {

class BaseCSVReader;

struct LineContext {
    uint64_t startByteOffset;
    uint64_t endByteOffset;

    bool isCompleteLine;

    void setNewLine(uint64_t start);
    void setEndOfLine(uint64_t end);
};

// If parsing in parallel during parsing we may not be able to determine line numbers
// Thus we have additional fields that can be used to determine line numbers + reconstruct lines
// After parsing this will be used to populate a PopulatedCSVError instance
struct CSVError {
    std::string message;
    std::string filePath;
    LineContext errorLine;

    uint64_t blockIdx;
    uint64_t numRowsReadInBlock;

    bool operator<(const CSVError& o) const;
};

struct PopulatedCSVError {
    std::string message;
    std::string filePath;
    std::string reconstructedLine;
    uint64_t lineNumber;
};

struct WarningCounter {
    uint64_t count;
};

class CSVErrorHandler {
public:
    explicit CSVErrorHandler(std::mutex* sharedMtx, uint64_t maxCachedErrorCount,
        WarningCounter* sharedWarningCounter, bool ignoreErrors);

    void handleError(BaseCSVReader* reader, const CSVError& error, bool mustThrow);
    void handleCachedErrors(BaseCSVReader* reader);

    void reportFinishedBlock(BaseCSVReader* reader, uint64_t blockIdx, uint64_t numRowsRead);
    void setHeaderNumRows(uint64_t numRows);

    uint64_t getCachedErrorCount();
    std::vector<PopulatedCSVError> getCachedErrors(BaseCSVReader* reader);

private:
    struct LinesPerBlock {
        uint64_t validLines;
        uint64_t invalidLines;
        bool doneParsingBlock;
    };

    common::UniqLock lock();
    void reportCachedErrors(BaseCSVReader* reader);
    void tryThrowFirstCachedError(BaseCSVReader* reader, bool mustThrow = false) const;

    std::string getErrorMessage(BaseCSVReader* reader, const CSVError& error,
        uint64_t lineNumber) const;
    PopulatedCSVError getPopulatedError(BaseCSVReader* reader, const CSVError& error,
        uint64_t lineNumber);
    void throwError(BaseCSVReader* reader, const CSVError& error, uint64_t lineNumber) const;
    bool canGetLineNumber(uint64_t blockIdx) const;
    void tryCacheError(const CSVError& error);
    uint64_t getLineNumber(uint64_t blockIdx, uint64_t numRowsReadInBlock) const;

    std::mutex* mtx; // can be nullptr, in which case mutual exclusion is guaranteed by the caller
    std::vector<LinesPerBlock> linesPerBlock;
    std::set<CSVError> cachedErrors;

    uint64_t maxCachedErrorCount;
    bool ignoreErrors;
    uint64_t headerNumRows;
    WarningCounter* sharedWarningCounter;
};
} // namespace kuzu::processor
