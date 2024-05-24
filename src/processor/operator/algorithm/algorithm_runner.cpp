#include "processor/operator/algorithm/algorithm_runner.h"

#include "common/cast.h"
#include "common/vector/value_vector.h"
#include "function/algorithm/graph_functions.h"
#include "processor/result/factorized_table.h"

using namespace kuzu::processor;

namespace kuzu {
namespace processor {

void AlgorithmRunner::initGlobalStateInternal(ExecutionContext*) {
    function::TableFunctionInitInput tableFunctionInitInput{info.bindData.get()};
    sharedState->funcState = info.function.initSharedStateFunc(tableFunctionInitInput);
}

void AlgorithmRunner::initLocalStateInternal(ResultSet*, ExecutionContext* context) {
    isMaster = parallelUtils->init();
    if (!isMaster) {
        // Init local state.
        localState = InQueryCallLocalState();
        localFTable = std::make_unique<FactorizedTable>(context->clientContext->getMemoryManager(),
            tableSchema->copy());
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
        localState.funcInput = function::TableFuncInput{
            info.bindData.get(), localState.funcState.get(), sharedState->funcState.get()};
    }
}

void AlgorithmRunner::executeInternal(ExecutionContext* executionContext) {
    if (isMaster) {
        bool ret = graphAlgorithm->compute(this, executionContext, parallelUtils);
    } else {
        runWorker();
    }
}

void AlgorithmRunner::runWorker() {
    while(true) {
        localState.funcOutput.dataChunk.state->selVector->selectedSize = 0;
        localState.funcOutput.dataChunk.resetAuxiliaryBuffer();
        auto numTuplesScanned = info.function.tableFuncs[sharedState->tableFuncIdx]
                                (localState.funcInput, localState.funcOutput);
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
            parallelUtils->mergeResults(localFTable.get());
            return;
        }
    }
}

} // namespace processor
} // namespace kuzu
