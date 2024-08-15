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

class CSVErrorHandler {
public:
    explicit CSVErrorHandler(std::mutex* sharedMtx, bool ignoreErrors);

    void reset();

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
    uint64_t getLineNumber(uint64_t blockIdx, uint64_t numRowsReadInBlock) const;

    std::mutex* mtx; // can be nullptr, in which case locking is handled by the caller
    std::vector<LinesPerBlock> linesPerBlock;
    std::set<CSVError> cachedErrors;

    bool ignoreErrors;
    uint64_t headerNumRows;
};
} // namespace kuzu::processor
