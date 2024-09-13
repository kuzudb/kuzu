#pragma once

#include "common/exception/copy.h"
#include "common/types/types.h"
#include "processor/execution_context.h"

namespace kuzu {
namespace processor {
struct BatchInsertCachedError {
    explicit BatchInsertCachedError(std::string message,
        std::optional<WarningSourceData> warningData = {});
    BatchInsertCachedError() = default;

    std::string message;

    // CSV Reader data
    std::optional<WarningSourceData> warningData;
};

class BatchInsertErrorHandler {
public:
    BatchInsertErrorHandler(ExecutionContext* context, bool ignoreErrors,
        std::shared_ptr<common::row_idx_t> sharedErrorCounter, std::mutex* sharedErrorCounterMtx);

    void handleError(BatchInsertCachedError error) {
        if (!ignoreErrors) {
            throw common::CopyException(error.message);
        }

        if (getNumErrors() >= warningLimit) {
            flushStoredErrors();
        }

        addNewVectorsIfNeeded();
        KU_ASSERT(currentInsertIdx < cachedErrors.size());
        cachedErrors[currentInsertIdx] = std::move(error);
        ++currentInsertIdx;
    }

    void flushStoredErrors();
    bool getIgnoreErrors() const;

private:
    common::row_idx_t getNumErrors() const;
    void addNewVectorsIfNeeded();
    void clearErrors();

    static constexpr uint64_t LOCAL_WARNING_LIMIT = 1024;

    bool ignoreErrors;
    uint64_t warningLimit;
    ExecutionContext* context;
    uint64_t currentInsertIdx;

    std::mutex* sharedErrorCounterMtx;
    std::shared_ptr<common::row_idx_t> sharedErrorCounter;

    std::vector<BatchInsertCachedError> cachedErrors;
};
} // namespace processor
} // namespace kuzu
