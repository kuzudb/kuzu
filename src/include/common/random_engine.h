#pragma once

#include <mutex>

#include "common/api.h"
#include "pcg_random.hpp"

namespace kuzu {

namespace main {
class ClientContext;
}

namespace common {

struct RandomState {
    pcg32 pcg;

    RandomState() {}
};

class KUZU_API RandomEngine {
public:
    RandomEngine();
    RandomEngine(uint64_t seed, uint64_t stream);

    uint32_t nextRandomInteger();
    uint32_t nextRandomInteger(uint32_t upper);

private:
    std::mutex mtx;
    RandomState randomState;
};

} // namespace common
} // namespace kuzu
