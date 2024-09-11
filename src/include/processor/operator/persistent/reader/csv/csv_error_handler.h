#pragma once

#include <map>
#include <string>
#include <vector>

#include "common/uniq_lock.h"
#include "processor/execution_context.h"
#include "processor/warning_context.h"

namespace kuzu::processor {

class BaseCSVReader;
class SerialCSVReader;

using warning_counter_t = uint64_t;

struct LinesPerBlock {
    uint64_t validLines;
    uint64_t invalidLines;
    bool doneParsingBlock;
};

class SharedCSVFileErrorHandler {
public:
    SharedCSVFileErrorHandler(std::string filePath, std::mutex* sharedMtx);

    void handleError(BaseCSVReader* reader, CSVError error);
    void throwCachedErrorsIfNeeded(BaseCSVReader* reader);

    void setHeaderNumRows(uint64_t numRows);

    void updateLineNumberInfo(const std::map<uint64_t, LinesPerBlock>& linesPerBlock);
    uint64_t getNumCachedErrors();
    uint64_t getLineNumber(uint64_t blockIdx, uint64_t numRowsReadInBlock) const;

    populate_func_t getPopulateCSVErrorFunc();

private:
    // this number can be small as we only cache errors if we wish to throw them later
    static constexpr uint64_t MAX_CACHED_ERROR_COUNT = 64;

    common::UniqLock lock();
    void tryThrowFirstCachedError(BaseCSVReader* reader);

    std::string getErrorMessage(BaseCSVReader* reader, CSVError error, uint64_t lineNumber) const;
    PopulatedCSVError getPopulatedError(BaseCSVReader* reader, CSVError error,
        uint64_t lineNumber) const;
    void throwError(BaseCSVReader* reader, CSVError error, uint64_t lineNumber) const;
    bool canGetLineNumber(uint64_t blockIdx) const;
    void tryCacheError(CSVError error, const common::UniqLock&);

    std::mutex* mtx; // can be nullptr, in which case mutual exclusion is guaranteed by the caller
    std::vector<LinesPerBlock> linesPerBlock;
    std::vector<CSVError> cachedErrors;
    std::string filePath;

    uint64_t headerNumRows;
};

class LocalCSVFileErrorHandler {
public:
    ~LocalCSVFileErrorHandler();

    LocalCSVFileErrorHandler(SharedCSVFileErrorHandler* sharedErrorHandler, bool ignoreErrors,
        main::ClientContext* context, bool cacheIgnoredErrors = true);

    void handleError(BaseCSVReader* reader, CSVError error);
    void reportFinishedBlock(uint64_t blockIdx, uint64_t numRowsRead);
    void setHeaderNumRows(uint64_t numRows);
    void finalize();

private:
    static constexpr uint64_t LOCAL_WARNING_LIMIT = 256;
    void flushCachedErrors();

    std::map<uint64_t, LinesPerBlock> linesPerBlock;
    std::vector<CSVError> cachedErrors;
    SharedCSVFileErrorHandler* sharedErrorHandler;
    main::ClientContext* context;

    uint64_t maxCachedErrorCount;
    bool ignoreErrors;
    bool cacheIgnoredErrors;
};

} // namespace kuzu::processor
