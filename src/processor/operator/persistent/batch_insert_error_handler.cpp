#include "processor/operator/persistent/batch_insert_error_handler.h"

#include "common/exception/copy.h"
#include "common/uniq_lock.h"
#include "processor/execution_context.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {
BatchInsertCachedError::BatchInsertCachedError(std::string message,
    std::optional<WarningSourceData> warningData)
    : message(message), warningData(warningData) {}

BatchInsertErrorHandler::BatchInsertErrorHandler(ExecutionContext* context, bool ignoreErrors,
    std::shared_ptr<common::row_idx_t> sharedErrorCounter, std::mutex* sharedErrorCounterMtx)
    : ignoreErrors(ignoreErrors),
      warningLimit(
          std::min(context->clientContext->getClientConfig()->warningLimit, LOCAL_WARNING_LIMIT)),
      context(context), currentInsertIdx(0), sharedErrorCounterMtx(sharedErrorCounterMtx),
      sharedErrorCounter(std::move(sharedErrorCounter)) {}

void BatchInsertErrorHandler::addNewVectorsIfNeeded() {
    KU_ASSERT(currentInsertIdx <= cachedErrors.size());
    if (currentInsertIdx == cachedErrors.size()) {
        cachedErrors.emplace_back();
    }
}

bool BatchInsertErrorHandler::getIgnoreErrors() const {
    return ignoreErrors;
}

void BatchInsertErrorHandler::handleError(std::string message,
    std::optional<WarningSourceData> warningData) {
    handleError(BatchInsertCachedError{std::move(message), std::move(warningData)});
}

void BatchInsertErrorHandler::handleError(BatchInsertCachedError error) {
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

void BatchInsertErrorHandler::flushStoredErrors() {
    std::map<uint64_t, std::vector<CSVError>> unpopulatedErrors;

    for (row_idx_t i = 0; i < getNumErrors(); ++i) {
        auto& error = cachedErrors[i];
        CSVError warningToAdd{std::move(error.message), {}, false};
        if (error.warningData.has_value()) {
            warningToAdd.completedLine = true;
            warningToAdd.warningData = error.warningData.value();
        }
        unpopulatedErrors[warningToAdd.warningData.fileIdx].push_back(warningToAdd);
    }

    uint64_t numErrorsFlushed = 0;
    for (const auto& [fileIdx, unpopulatedErrorsByFile] : unpopulatedErrors) {
        KU_ASSERT(ignoreErrors);
        if (!unpopulatedErrorsByFile.empty()) {
            context->clientContext->getWarningContextUnsafe().appendWarningMessages(
                unpopulatedErrorsByFile);
            numErrorsFlushed += unpopulatedErrorsByFile.size();
        }
    }

    if (numErrorsFlushed > 0 && sharedErrorCounter != nullptr) {
        KU_ASSERT(sharedErrorCounterMtx);
        common::UniqLock lockGuard{*sharedErrorCounterMtx};
        *sharedErrorCounter += numErrorsFlushed;
    }

    clearErrors();
}

void BatchInsertErrorHandler::clearErrors() {
    currentInsertIdx = 0;
}

row_idx_t BatchInsertErrorHandler::getNumErrors() const {
    return currentInsertIdx;
}

} // namespace processor
} // namespace kuzu
