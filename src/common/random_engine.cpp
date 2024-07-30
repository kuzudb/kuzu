#include "common/random_engine.h"

#include <random>

namespace kuzu {
namespace common {

RandomEngine::RandomEngine() : randomState(RandomState()) {
    randomState.pcg.seed(pcg_extras::seed_seq_from<std::random_device>());
}

uint32_t RandomEngine::nextRandomInteger() {
    return randomState.pcg();
}

uint32_t RandomEngine::nextRandomInteger(uint32_t upper) {
    return randomState.pcg(upper);
}

} // namespace common
} // namespace kuzu
