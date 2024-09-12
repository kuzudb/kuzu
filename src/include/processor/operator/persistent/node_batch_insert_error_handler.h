#pragma once

#include "common/exception/copy.h"
#include "common/types/types.h"
#include "processor/execution_context.h"

namespace kuzu {
namespace storage {
class NodeTable;
}

namespace processor {
template<typename T>
struct IndexBuilderError {
    std::string message;
    T key;
    common::nodeID_t nodeID;

    // CSV Reader data
    std::optional<WarningSourceData> warningData;
};

struct IndexBuilderCachedError {
    IndexBuilderCachedError(std::string message, std::optional<WarningSourceData> warningData);
    IndexBuilderCachedError() = default;

    template<typename T>
    static IndexBuilderCachedError constructFrom(IndexBuilderError<T> error) {
        return IndexBuilderCachedError{std::move(error.message), std::move(error.warningData)};
    }

    std::string message;

    // CSV Reader data
    std::optional<WarningSourceData> warningData;
};

class NodeBatchInsertErrorHandler {
public:
    NodeBatchInsertErrorHandler(ExecutionContext* context, common::LogicalTypeID pkType,
        storage::NodeTable* nodeTable, bool ignoreErrors,
        std::shared_ptr<common::row_idx_t> sharedErrorCounter, std::mutex* sharedErrorCounterMtx);

    template<typename T>
    void handleError(IndexBuilderError<T> error) {
        if (!ignoreErrors) {
            throw common::CopyException(error.message);
        }

        setCurrentErroneousRow(error.key, error.nodeID);
        deleteCurrentErroneousRow();

        if (getNumErrors() >= warningLimit) {
            flushStoredErrors();
        }

        addNewVectorsIfNeeded();
        KU_ASSERT(currentInsertIdx < cachedErrors.size());
        cachedErrors[currentInsertIdx] = IndexBuilderCachedError::constructFrom(std::move(error));
        ++currentInsertIdx;
    }

    void flushStoredErrors();

private:
    common::row_idx_t getNumErrors() const;
    void addNewVectorsIfNeeded();
    void clearErrors();

    template<typename T>
    void setCurrentErroneousRow(const T& key, common::nodeID_t nodeID) {
        keyVector->setValue<T>(0, key);
        offsetVector->setValue(0, nodeID);
    }

    void deleteCurrentErroneousRow();

    static constexpr common::idx_t DELETE_VECTOR_SIZE = 1;
    static constexpr uint64_t LOCAL_WARNING_LIMIT = 1024;

    bool ignoreErrors;
    uint64_t warningLimit;
    ExecutionContext* context;
    storage::NodeTable* nodeTable;
    uint64_t queryID;
    uint64_t currentInsertIdx;

    std::mutex* sharedErrorCounterMtx;
    std::shared_ptr<common::row_idx_t> sharedErrorCounter;

    // vectors that are reused by each deletion
    std::shared_ptr<common::ValueVector> keyVector;
    std::shared_ptr<common::ValueVector> offsetVector;

    std::vector<IndexBuilderCachedError> cachedErrors;
};
} // namespace processor
} // namespace kuzu
