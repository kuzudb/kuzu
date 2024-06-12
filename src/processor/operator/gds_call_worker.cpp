#include "processor/operator/gds_call_worker.h"

#include "processor/operator/sink.h"

namespace kuzu {
namespace processor {

void GDSCallWorker::initLocalStateInternal(ResultSet*, ExecutionContext* context) {
    info.gds->init(sharedState->graph.get(), sharedState->fTable.get(), context->clientContext);
}

void GDSCallWorker::executeInternal(ExecutionContext*  /*context*/) {
    while (true) {
        auto numTuplesScanned = funcToExecute
    }

    while (true) {
        localState.funcOutput.dataChunk.state->selVector->selectedSize = 0;
        localState.funcOutput.dataChunk.resetAuxiliaryBuffer();
        auto numTuplesScanned = funcToExecute(localState.funcInput, localState.funcOutput);
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
