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
    // Init local state.
    localState = InQueryCallLocalState();
    // Init table function output.
    switch (info.outputType) {
    case TableScanOutputType::EMPTY:
        break; // Do nothing.
    case TableScanOutputType::SINGLE_DATA_CHUNK: {
        KU_ASSERT(!info.outPosV.empty());
        auto state = resultSet->getDataChunk(info.outPosV[0].dataChunkPos)->state;
        localState.funcOutput.dataChunk = DataChunk(info.outPosV.size(), state);
        for (auto i = 0u; i < info.outPosV.size(); ++i) {
            localState.funcOutput.dataChunk.insert(i, resultSet->getValueVector(info.outPosV[i]));
        }
    } break;
    case TableScanOutputType::MULTI_DATA_CHUNK: {
        for (auto& pos : info.outPosV) {
            localState.funcOutput.vectors.push_back(resultSet->getValueVector(pos).get());
        }
    } break;
    default:
        KU_UNREACHABLE;
    }
    if (info.rowOffsetPos.isValid()) {
        localState.rowOffsetVector = resultSet->getValueVector(info.rowOffsetPos).get();
    }
    // Init table function input.
    function::TableFunctionInitInput tableFunctionInitInput{info.bindData.get()};
    localState.funcState = info.function.initLocalStateFunc(tableFunctionInitInput,
        sharedState->funcState.get(), context->clientContext->getMemoryManager());
    localState.funcInput = function::TableFuncInput{
        info.bindData.get(), localState.funcState.get(), sharedState->funcState.get()};
}

void InQueryCall::initGlobalStateInternal(ExecutionContext*) {
    function::TableFunctionInitInput tableFunctionInitInput{info.bindData.get()};
    sharedState->funcState = info.function.initSharedStateFunc(tableFunctionInitInput);
}

bool InQueryCall::getNextTuplesInternal(ExecutionContext*) {
    localState.funcOutput.dataChunk.state->selVector->selectedSize = 0;
    localState.funcOutput.dataChunk.resetAuxiliaryBuffer();
    auto numTuplesScanned = info.function.tableFunc(localState.funcInput, localState.funcOutput);
    localState.funcOutput.dataChunk.state->selVector->selectedSize = numTuplesScanned;
    if (localState.rowOffsetVector != nullptr) {
        auto rowIdx = sharedState->getAndIncreaseRowIdx(numTuplesScanned);
        for (auto i = 0u; i < numTuplesScanned; i++) {
            localState.rowOffsetVector->setValue(i, rowIdx + i);
        }
    }
    return numTuplesScanned != 0;
}

} // namespace processor
} // namespace kuzu
