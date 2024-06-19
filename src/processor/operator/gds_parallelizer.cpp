#include "processor/operator/gds_parallelizer.h"

#include "processor/operator/sink.h"
// TODO(Semih): Remove
#include <iostream>

namespace kuzu {
namespace processor {

void GDSParallelizer::initLocalStateInternal(ResultSet*, ExecutionContext* context) {
    //    info.gds->init(sharedState.get(), context->clientContext);
    //    auto memoryManager = context->clientContext->getMemoryManager();
    //    auto& ftableSchema = *sharedState->fTable->getTableSchema();
    //    localFTable = std::make_unique<FactorizedTable>(memoryManager, ftableSchema.copy());
}

void GDSParallelizer::executeInternal(ExecutionContext* /*context*/) {
    std::cout << std::this_thread::get_id() << " GDSParallelizer::executeInternal()" << std::endl;
    uint64_t localSum = 0;
    while (true) {
        // Get the next morsel to process
        auto morsel = sharedState->nextMorsel.fetch_add(GDSParallelizerSharedState::MORSEL_SIZE);
        std::cout << std::this_thread::get_id() << " morsel: " << morsel << std::endl;
        if (morsel >= sharedState->count) {
            break;
        } else {
            for (auto i = morsel; i < morsel + GDSParallelizerSharedState::MORSEL_SIZE; i++) {
                localSum += i;
            }
        }
    }
    sharedState->sum.fetch_add(localSum);
    std::cout << std::this_thread::get_id() << " printing: localSum: " << localSum
              << ": totalSum: " << sharedState->sum.load() << std::endl;
}

} // namespace processor
} // namespace kuzu