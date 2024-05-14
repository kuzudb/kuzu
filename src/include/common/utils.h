#pragma once

#include <cstdint>
#include <vector>

#include "common/assert.h"

namespace kuzu {
namespace common {

class BitmaskUtils {

public:
    static uint64_t all1sMaskForLeastSignificantBits(uint64_t numBits) {
        KU_ASSERT(numBits <= 64);
        return numBits == 64 ? UINT64_MAX : ((uint64_t)1 << numBits) - 1;
    }
};

uint64_t nextPowerOfTwo(uint64_t v);
uint64_t prevPowerOfTwo(uint64_t v);

bool isLittleEndian();

template<typename T>
bool integerFitsIn(int64_t val);

template<typename T>
std::vector<T> copyVector(const std::vector<T>& objects) {
    std::vector<T> result;
    result.reserve(objects.size());
    for (auto& object : objects) {
        result.push_back(object->copy());
    }
    return result;
}

} // namespace common
} // namespace kuzu
