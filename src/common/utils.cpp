#include "common/utils.h"

namespace kuzu {
namespace common {

uint64_t nextPowerOfTwo(uint64_t v) {
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v |= v >> 32;
    v++;
    return v;
}

uint64_t prevPowerOfTwo(uint64_t v) {
    return nextPowerOfTwo((v / 2) + 1);
}

bool isLittleEndian() {
    // Little endian arch stores the least significant value in the lower bytes.
    int testNumber = 1;
    return *(uint8_t*)&testNumber == 1;
}

template<>
bool integerFitsIn<int64_t>(int64_t) {
    return true;
}

template<>
bool integerFitsIn<int32_t>(int64_t val) {
    return val >= INT32_MIN && val <= INT32_MAX;
}

template<>
bool integerFitsIn<int16_t>(int64_t val) {
    return val >= INT16_MIN && val <= INT16_MAX;
}

template<>
bool integerFitsIn<int8_t>(int64_t val) {
    return val >= INT8_MIN && val <= INT8_MAX;
}

template<>
bool integerFitsIn<uint64_t>(int64_t val) {
    return val >= 0;
}

template<>
bool integerFitsIn<uint32_t>(int64_t val) {
    return val >= 0 && val <= UINT32_MAX;
}

template<>
bool integerFitsIn<uint16_t>(int64_t val) {
    return val >= 0 && val <= UINT16_MAX;
}

template<>
bool integerFitsIn<uint8_t>(int64_t val) {
    return val >= 0 && val <= UINT8_MAX;
}

} // namespace common
} // namespace kuzu
