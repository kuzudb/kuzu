#include "common/random_engine.h"

#include <random>

namespace kuzu {
namespace common {

RandomEngine::RandomEngine() : randomState(RandomState()) {
    randomState.pcg.seed(pcg_extras::seed_seq_from<std::random_device>());
}

RandomEngine::RandomEngine(uint64_t seed, uint64_t stream) : randomState(RandomState()) {
    randomState.pcg.seed(seed, stream);
}

uint32_t RandomEngine::nextRandomInteger() {
    std::unique_lock xLck{mtx};
    return randomState.pcg();
}

uint32_t RandomEngine::nextRandomInteger(uint32_t upper) {
    std::unique_lock xLck{mtx};
    return randomState.pcg(upper);
}

} // namespace common
} // namespace kuzu
