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
};

class NodeBatchInsertErrorHandler {
public:
    NodeBatchInsertErrorHandler(ExecutionContext* context, common::LogicalTypeID pkType,
        storage::NodeTable* nodeTable, std::shared_ptr<common::row_idx_t> sharedErrorCounter,
        std::mutex* sharedErrorCounterMtx);

    template<typename T>
    void handleError(IndexBuilderError<T> error) {
        if (!ignoreErrors) {
            throw common::CopyException(error.message);
        }

        if (getNumErrors() >= warningLimit) {
            flushStoredErrors();
        }

        addNewVectors();
        keyVector.back()->setValue<T>(0, error.key);
        offsetVector.back()->setValue(0, error.nodeID);
        errorMessages.emplace_back(std::move(error.message));
    }

    void flushStoredErrors();

private:
    common::row_idx_t getNumErrors() const;
    void addNewVectors();
    void clearErrors();

    static constexpr common::idx_t DELETE_VECTOR_SIZE = 1;
    static constexpr uint64_t LOCAL_WARNING_LIMIT = 1024;

    bool ignoreErrors;
    uint64_t warningLimit;
    ExecutionContext* context;
    common::LogicalTypeID pkType;
    storage::NodeTable* nodeTable;
    uint64_t queryID;

    std::mutex* sharedErrorCounterMtx;
    std::shared_ptr<common::row_idx_t> sharedErrorCounter;

    std::vector<std::shared_ptr<common::ValueVector>> keyVector;
    std::vector<std::shared_ptr<common::ValueVector>> offsetVector;
    std::vector<std::string> errorMessages;
};
} // namespace processor
} // namespace kuzu
