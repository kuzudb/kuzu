#include "function/algorithm/graph_functions.h"
#include "processor/operator/algorithm/algorithm_runner.h"
#include "common/cast.h"

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
        // Init table function output.
        switch (info.outputType) {
        case TableScanOutputType::EMPTY:
            break; // Do nothing.
        case TableScanOutputType::SINGLE_DATA_CHUNK: {
            KU_ASSERT(!info.outPosV.empty());
            auto state = resultSet->getDataChunk(info.outPosV[0].dataChunkPos)->state;
            localState.funcOutput.dataChunk = common::DataChunk(info.outPosV.size(), state);
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
        localState.funcInput = function::TableFuncInput{info.bindData.get(), localState.funcState.get(),
            sharedState->funcState.get()};
    }
}

bool AlgorithmRunner::getNextTuplesInternal(ExecutionContext*) {
    if (isMaster) {
        bool ret = graphAlgorithm->compute(parallelUtils.get(), info.function);
        printf("master returning %d from here ...\n", ret);
        return ret;
    } else {
        localState.funcOutput.dataChunk.state->selVector->selectedSize = 0;
        localState.funcOutput.dataChunk.resetAuxiliaryBuffer();
        bool ret = parallelUtils->runWorkerThread(localState.funcInput, localState.funcOutput);
        printf("worker returning %d from here ...\n", ret);
        return ret;
    }
}

}
}
