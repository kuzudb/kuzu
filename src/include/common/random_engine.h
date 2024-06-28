#pragma once

#include "pcg_random.hpp"

namespace kuzu {

namespace main {
class ClientContext;
}

namespace common {

struct RandomState {
    pcg64 pcg;

    RandomState() {}
};

class RandomEngine {
public:
    RandomEngine();

    uint32_t nextRandomInteger();
    float randomFloat();
    uint64_t randomInt(uint64_t max);
    void randomPermutation(uint64_t n, uint64_t* perm, uint64_t nPerm);

private:
    RandomState randomState;
};

} // namespace common
} // namespace kuzu
