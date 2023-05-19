#pragma once

#include "common/in_mem_overflow_buffer.h"
#include "type_utils.h"

namespace kuzu {
namespace common {

class InMemOverflowBufferUtils {
public:
    // Currently, this function is only used in `allocateStringIfNecessary`. Make this function
    // private once we remove allocateStringIfNecessary.
    static inline void allocateSpaceForStringIfNecessary(
        ku_string_t& result, uint64_t numBytes, InMemOverflowBuffer& buffer) {
        if (ku_string_t::isShortString(numBytes)) {
            return;
        }
        result.overflowPtr = reinterpret_cast<uint64_t>(buffer.allocateSpace(numBytes));
    }

    static void copyString(
        const char* src, uint64_t len, ku_string_t& dest, InMemOverflowBuffer& inMemOverflowBuffer);
    static void copyString(
        const ku_string_t& src, ku_string_t& dest, InMemOverflowBuffer& inMemOverflowBuffer);
};

} // namespace common
} // namespace kuzu
