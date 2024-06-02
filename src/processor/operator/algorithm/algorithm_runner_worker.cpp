#include "processor/operator/algorithm/algorithm_runner_worker.h"

#include "processor/operator/sink.h"

namespace kuzu {
namespace processor {

void AlgorithmRunnerWorker::initLocalStateInternal(ResultSet*, ExecutionContext* context) {
    // Init local state.
    localState = InQueryCallLocalState();
    localFTable = std::make_unique<FactorizedTable>(context->clientContext->getMemoryManager(),
        algorithmRunnerSharedState->resultFTable->getTableSchema()->copy());
    // Init table function output.
    switch (info.outputType) {
    case TableScanOutputType::EMPTY:
        break; // Do nothing.
    case TableScanOutputType::SINGLE_DATA_CHUNK: {
        KU_ASSERT(!info.outPosV.empty());
        auto state = resultSet->getDataChunk(info.outPosV[0].dataChunkPos)->state;
        localState.funcOutput.dataChunk = common::DataChunk(info.outPosV.size(), state);
        for (auto i = 0u; i < info.outPosV.size(); ++i) {
            auto valueVector = resultSet->getValueVector(info.outPosV[i]);
            outputVectors.push_back(valueVector.get());
            localState.funcOutput.dataChunk.insert(i, valueVector);
        }
    } break;
    case TableScanOutputType::MULTI_DATA_CHUNK: {
        for (auto& pos : info.outPosV) {
            auto valueVector = resultSet->getValueVector(pos);
            outputVectors.push_back(valueVector.get());
            localState.funcOutput.vectors.push_back(resultSet->getValueVector(pos).get());
        }
    } break;
    default:
        KU_UNREACHABLE;
    }
    if (info.rowOffsetPos.isValid()) {
        localState.rowOffsetVector = resultSet->getValueVector(info.rowOffsetPos).get();
        outputVectors.push_back(localState.rowOffsetVector);
    }
    // Init table function input.
    function::TableFunctionInitInput tableFunctionInitInput{info.bindData.get()};
    localState.funcState = info.function.initLocalStateFunc(tableFunctionInitInput,
        sharedState->funcState.get(), context->clientContext->getMemoryManager());
    localState.funcInput = function::TableFuncInput{info.bindData.get(), localState.funcState.get(),
        sharedState->funcState.get()};
}

void AlgorithmRunnerWorker::executeInternal(ExecutionContext*  /*context*/) {
    while (true) {
        localState.funcOutput.dataChunk.state->selVector->selectedSize = 0;
        localState.funcOutput.dataChunk.resetAuxiliaryBuffer();
        auto numTuplesScanned = info.function.tableFuncs[sharedState->tableFuncIdx](
            localState.funcInput, localState.funcOutput);
        localState.funcOutput.dataChunk.state->selVector->selectedSize = numTuplesScanned;
        if (localState.rowOffsetVector != nullptr) {
            auto rowIdx = sharedState->getAndIncreaseRowIdx(numTuplesScanned);
            for (auto i = 0u; i < numTuplesScanned; i++) {
                localState.rowOffsetVector->setValue(i, rowIdx + i);
            }
        }
        if (numTuplesScanned && !outputVectors.empty()) {
            localFTable->append(outputVectors);
        } else {
            algorithmRunnerSharedState->mergeResults(localFTable.get());
            return;
        }
    }
}

} // namespace processor
} // namespace kuzu
