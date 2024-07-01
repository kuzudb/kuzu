#include "processor/operator/parallel_utils_operator.h"

#include "processor/operator/sink.h"

namespace kuzu {
namespace processor {

void ParallelUtilsOperator::initLocalStateInternal(ResultSet*, ExecutionContext* context) {
    gdsLocalState->init(context->clientContext);
    auto memoryManager = context->clientContext->getMemoryManager();
    auto& ftableSchema = *sharedState->fTable->getTableSchema();
    localFTable = std::make_unique<FactorizedTable>(memoryManager, ftableSchema.copy());
}

void ParallelUtilsOperator::executeInternal(ExecutionContext* /*context*/) {
    auto& outputVectors = gdsLocalState->getOutputVectors();
    while (true) {
        auto numTuplesOutput = funcToExecute(sharedState, gdsLocalState.get());
        switch (numTuplesOutput) {
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
