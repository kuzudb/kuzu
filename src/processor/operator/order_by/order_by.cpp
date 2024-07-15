#include "processor/operator/order_by/order_by.h"

#include "binder/expression/expression_util.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

std::string OrderByPrintInfo::toString() const {
    std::string result = "Order By: ";
    result += binder::ExpressionUtil::toString(keys);
    result += ", Expressions: ";
    result += binder::ExpressionUtil::toString(payloads);
    return result;
}

void OrderBy::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    localState = std::make_unique<SortLocalState>();
    localState->init(*info, *sharedState, context->clientContext->getMemoryManager());
    for (auto& dataPos : info->payloadsPos) {
        payloadVectors.push_back(resultSet->getValueVector(dataPos).get());
    }
    for (auto& dataPos : info->keysPos) {
        orderByVectors.push_back(resultSet->getValueVector(dataPos).get());
    }
}

void OrderBy::initGlobalStateInternal(ExecutionContext* /*context*/) {
    sharedState->init(*info);
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
