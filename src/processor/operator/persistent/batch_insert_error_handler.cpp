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
    cachedErrors[currentInsertIdx] = std::move(error);
    ++currentInsertIdx;
}

void BatchInsertErrorHandler::flushStoredErrors() {
    std::vector<CopyFromFileError> unpopulatedErrors;

    for (row_idx_t i = 0; i < getNumErrors(); ++i) {
        auto& error = cachedErrors[i];
        CopyFromFileError warningToAdd{std::move(error.message), {}, false};
        if (error.warningData.has_value()) {
            warningToAdd.completedLine = true;
            warningToAdd.warningData = std::move(error.warningData.value());
        }
        unpopulatedErrors.push_back(warningToAdd);
    }

    if (!unpopulatedErrors.empty()) {
        KU_ASSERT(ignoreErrors);
        context->clientContext->getWarningContextUnsafe().appendWarningMessages(unpopulatedErrors);
    }

    if (!unpopulatedErrors.empty() && sharedErrorCounter != nullptr) {
        KU_ASSERT(sharedErrorCounterMtx);
        common::UniqLock lockGuard{*sharedErrorCounterMtx};
        *sharedErrorCounter += unpopulatedErrors.size();
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
