#include "function/gds/gds_task.h"
// TODO(Semih): Remove
#include <iostream>
#include <thread>

namespace kuzu {
namespace function {

void GDSTask::run() {
    std::cout << std::this_thread::get_id() << " GDSParallelizer::executeInternal()" << std::endl;
    uint64_t localSum = 0;
    while (true) {
        // Get the next morsel to process
        auto morsel = sharedState->nextMorsel.fetch_add(GDSTaskSharedState::MORSEL_SIZE);
        if (morsel >= sharedState->count) {
            break;
        } else {
            for (auto i = morsel; i < morsel + GDSTaskSharedState::MORSEL_SIZE; i++) {
                localSum += i;
            }
        }
    }
    sharedState->sum.fetch_add(localSum);
    std::cout << std::this_thread::get_id() << " printing: localSum: " << localSum
              << ": totalSum: " << sharedState->sum.load() << std::endl;
}

} // namespace function
} // namespace kuzu