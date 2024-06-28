#include "common/random_engine.h"

#include <random>

#include "common/assert.h"
#include <unordered_map>

namespace kuzu {
namespace common {

RandomEngine::RandomEngine() : randomState(RandomState()) {
    randomState.pcg.seed(pcg_extras::seed_seq_from<std::random_device>());
}

uint32_t RandomEngine::nextRandomInteger() {
    return randomState.pcg() % UINT32_MAX;
}

uint64_t RandomEngine::randomInt(uint64_t max) {
    return randomState.pcg() % max;
}

float RandomEngine::randomFloat() {
    return randomState.pcg() / float(randomState.pcg.max());
}

void RandomEngine::randomPermutation(uint64_t n, uint64_t* perm, uint64_t nPerm) {
    KU_ASSERT(nPerm <= n);
    std::unordered_map<uint64_t, uint64_t> m;
    for (uint64_t i = 0; i < nPerm - 1; i++) {
        auto i2 = i + randomInt(n - i);
        if (m.contains(i2)) {
            perm[i] = m[i2];
        } else {
            perm[i] = i2;
        }
        m[i2] = i;
    }

    // last element
    if (m.contains(nPerm - 1)) {
        perm[nPerm - 1] = m[nPerm - 1];
    } else {
        perm[nPerm - 1] = nPerm - 1;
    }
}

} // namespace common
} // namespace kuzu
