#include "processor/operator/order_by/order_by.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void OrderBy::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    localState->init(orderByDataInfo, *sharedState, context->memoryManager);
    for (auto [dataPos, _] : orderByDataInfo.payloadsPosAndType) {
        payloadVectors.push_back(resultSet->getValueVector(dataPos).get());
    }
    for (auto [dataPos, _] : orderByDataInfo.keysPosAndType) {
        orderByVectors.push_back(resultSet->getValueVector(dataPos).get());
    }
}

void OrderBy::initGlobalStateInternal(ExecutionContext* context) {
    sharedState->init(orderByDataInfo);
}

void OrderBy::executeInternal(ExecutionContext* context) {
    // Append thread-local tuples.
    while (children[0]->getNextTuple(context)) {
        for (auto i = 0u; i < resultSet->multiplicity; i++) {
            localState->append(orderByVectors, payloadVectors);
        }
    }
    localState->finalize(*sharedState);
}

} // namespace processor
} // namespace kuzu
