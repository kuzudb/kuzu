#include "processor/operator/call/in_query_call.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

common::row_idx_t InQueryCallSharedState::getAndIncreaseRowIdx(uint64_t numRows) {
    std::lock_guard lock{mtx};
    auto curRowIdx = nextRowIdx;
    nextRowIdx += numRows;
    return curRowIdx;
}

void InQueryCall::initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) {
    localState = std::make_unique<InQueryCallLocalState>();
    if (!inQueryCallInfo->outputPoses.empty()) {
        localState->outputChunk = std::make_unique<DataChunk>(inQueryCallInfo->outputPoses.size(),
            resultSet->getDataChunk(inQueryCallInfo->outputPoses[0].dataChunkPos)->state);
    } else {
        // Some scan function (e.g. rdf scan) doesn't output any vector.
        localState->outputChunk = std::make_unique<DataChunk>(0);
    }
    for (auto i = 0u; i < inQueryCallInfo->outputPoses.size(); i++) {
        localState->outputChunk->insert(
            i, resultSet->getValueVector(inQueryCallInfo->outputPoses[i]));
    }
    localState->rowIDVector = resultSet->getValueVector(inQueryCallInfo->rowIDPos).get();
    function::TableFunctionInitInput tableFunctionInitInput{inQueryCallInfo->bindData.get()};
    localState->localState = inQueryCallInfo->function->initLocalStateFunc(tableFunctionInitInput,
        sharedState->sharedState.get(), context->clientContext->getMemoryManager());
    localState->tableFunctionInput = function::TableFunctionInput{inQueryCallInfo->bindData.get(),
        localState->localState.get(), sharedState->sharedState.get()};
}

void InQueryCall::initGlobalStateInternal(ExecutionContext* /*context*/) {
    function::TableFunctionInitInput tableFunctionInitInput{inQueryCallInfo->bindData.get()};
    sharedState->sharedState =
        inQueryCallInfo->function->initSharedStateFunc(tableFunctionInitInput);
}

bool InQueryCall::getNextTuplesInternal(ExecutionContext* /*context*/) {
    localState->outputChunk->state->selVector->selectedSize = 0;
    localState->outputChunk->resetAuxiliaryBuffer();
    inQueryCallInfo->function->tableFunc(localState->tableFunctionInput, *localState->outputChunk);
    auto numRowsToOutput = localState->outputChunk->state->selVector->selectedSize;
    auto rowIdx = sharedState->getAndIncreaseRowIdx(numRowsToOutput);
    for (auto i = 0u; i < numRowsToOutput; i++) {
        localState->rowIDVector->setValue(i, rowIdx + i);
    }
    return localState->outputChunk->state->selVector->selectedSize != 0;
}

} // namespace processor
} // namespace kuzu
