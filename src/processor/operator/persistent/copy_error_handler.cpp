#include "processor/operator/persistent/copy_error_handler.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {
IndexBuilderErrorHandler::IndexBuilderErrorHandler(main::ClientContext* clientContext,
    common::LogicalTypeID pkType)
    : ignoreErrors(clientContext->getClientConfig()->ignoreCopyErrors),
      clientContext(clientContext), pkType(pkType) {}

void IndexBuilderErrorHandler::append(const IndexBuilderErrorHandler& errors) {
    for (idx_t i = 0; i < errors.offsetVector.size(); ++i) {
        offsetVector.insert(offsetVector.end(), errors.offsetVector.begin(),
            errors.offsetVector.end());
        keyVector.insert(keyVector.end(), errors.keyVector.begin(), errors.keyVector.end());
    }
}

void IndexBuilderErrorHandler::addNewVectors() {
    offsetVector.push_back(std::make_shared<ValueVector>(LogicalTypeID::INTERNAL_ID,
        clientContext->getMemoryManager()));
    offsetVector.back()->state = DataChunkState::getSingleValueDataChunkState();
    keyVector.push_back(std::make_shared<ValueVector>(pkType, clientContext->getMemoryManager()));
    keyVector.back()->state = DataChunkState::getSingleValueDataChunkState();
}

std::vector<std::shared_ptr<common::ValueVector>>& IndexBuilderErrorHandler::getKeyVector() {
    return keyVector;
}

std::vector<std::shared_ptr<common::ValueVector>>& IndexBuilderErrorHandler::getOffsetVector() {
    return offsetVector;
}

} // namespace processor
} // namespace kuzu
