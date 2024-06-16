#include "processor/operator/gds_call_worker.h"

#include "processor/operator/sink.h"

namespace kuzu {
namespace processor {

void GDSCallWorker::initLocalStateInternal(ResultSet*, ExecutionContext* context) {
    info.gds->init(sharedState.get(), context->clientContext);
    auto memoryManager = context->clientContext->getMemoryManager();
    auto& ftableSchema = *sharedState->fTable->getTableSchema();
    localFTable = std::make_unique<FactorizedTable>(memoryManager, ftableSchema.copy());
}

void GDSCallWorker::executeInternal(ExecutionContext* /*context*/) {
    auto localState = info.gds->getGDSLocalState();
    auto fTable = sharedState->fTable;
    auto& outputVectors = localState->getOutputVectors();
    while (true) {
        auto numTuplesScanned = funcToExecute(sharedState, localState);
        switch (numTuplesScanned) {
        case 0:
            sharedState->merge(*localFTable.get());
            return;
        case UINT64_MAX:
            continue;
        default:
            if (!outputVectors.empty()) {
                localFTable->append(outputVectors);
            }
        }
    }
}

} // namespace processor
} // namespace kuzu
