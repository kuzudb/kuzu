#include "common/in_mem_overflow_buffer_utils.h"

namespace kuzu {
namespace common {

void InMemOverflowBufferUtils::copyString(
    const char* src, uint64_t len, ku_string_t& dest, InMemOverflowBuffer& inMemOverflowBuffer) {
    InMemOverflowBufferUtils::allocateSpaceForStringIfNecessary(dest, len, inMemOverflowBuffer);
    dest.set(src, len);
}

void InMemOverflowBufferUtils::copyString(
    const ku_string_t& src, ku_string_t& dest, InMemOverflowBuffer& inMemOverflowBuffer) {
    InMemOverflowBufferUtils::allocateSpaceForStringIfNecessary(dest, src.len, inMemOverflowBuffer);
    dest.set(src);
}

} // namespace common
} // namespace kuzu
