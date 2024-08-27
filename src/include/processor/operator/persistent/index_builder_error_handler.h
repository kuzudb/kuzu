#include "common/exception/copy.h"
#include "common/types/types.h"
#include "main/client_context.h"

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

class IndexBuilderErrorHandler {
public:
    IndexBuilderErrorHandler(ExecutionContext* context, common::LogicalTypeID pkType,
        storage::NodeTable* nodeTable, uint64_t queryID);

    template<typename T>
    void handleOrStoreError(IndexBuilderError<T> error) {
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
    static constexpr common::idx_t DELETE_VECTOR_SIZE = 1;
    static constexpr uint64_t LOCAL_WARNING_LIMIT = 1024;

    bool ignoreErrors;
    uint64_t warningLimit;
    ExecutionContext* context;
    common::LogicalTypeID pkType;
    storage::NodeTable* nodeTable;
    uint64_t queryID;

    std::vector<std::shared_ptr<common::ValueVector>> keyVector;
    std::vector<std::shared_ptr<common::ValueVector>> offsetVector;
    std::vector<std::string> errorMessages;

    uint64_t getNumErrors() const;
    void addNewVectors();
    void clearErrors();
};
} // namespace processor
} // namespace kuzu
