#include "common/exception/copy.h"
#include "common/types/types.h"
#include "main/client_context.h"

namespace kuzu {
namespace processor {
template<typename T>
struct IndexBuilderError {
    std::string message;
    T key;
    common::nodeID_t nodeID;
};

class IndexBuilderErrorHandler {
public:
    IndexBuilderErrorHandler(main::ClientContext* clientContext, common::LogicalTypeID pkType);

    template<typename T>
    void handleOrStoreError(IndexBuilderError<T> error) {
        if (!ignoreErrors) {
            throw common::CopyException(error.message);
        }
        addNewVectors();
        keyVector.back()->setValue<T>(0, error.key);
        offsetVector.back()->setValue(0, error.nodeID);
        errorMessages.emplace_back(std::move(error.message));
    }

    void handleStoredErrors();

    void append(const IndexBuilderErrorHandler& errors);

    std::vector<std::shared_ptr<common::ValueVector>>& getKeyVector();
    std::vector<std::shared_ptr<common::ValueVector>>& getOffsetVector();

private:
    static constexpr common::idx_t DELETE_VECTOR_SIZE = 1;

    bool ignoreErrors;
    main::ClientContext* clientContext;
    common::LogicalTypeID pkType;

    std::vector<std::shared_ptr<common::ValueVector>> keyVector;
    std::vector<std::shared_ptr<common::ValueVector>> offsetVector;
    std::vector<std::string> errorMessages;

    void addNewVectors();
};
} // namespace processor
} // namespace kuzu
