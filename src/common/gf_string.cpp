#include "src/common/include/gf_string.h"

#include <cstring>

namespace graphflow {
namespace common {

void gf_string_t::setOverflowPtrInfo(const uint64_t& pageIdx, const uint16_t& pageOffset) {
    memcpy(&overflowPtr, &pageIdx, 6);
    memcpy(((uint8_t*)&overflowPtr) + 6, &pageOffset, 2);
}

void gf_string_t::getOverflowPtrInfo(uint64_t& pageIdx, uint16_t& pageOffset) {
    pageIdx = 0;
    memcpy(&pageIdx, &overflowPtr, 6);
    memcpy(&pageOffset, ((uint8_t*)&overflowPtr) + 6, 2);
}

void gf_string_t::set(const string& str) {
    set(str.data(), str.length());
}

void gf_string_t::set(const char* value, uint64_t length) {
    auto prefixLen = length < gf_string_t::PREFIX_LENGTH ? length : gf_string_t::PREFIX_LENGTH;
    memcpy(prefix, value, prefixLen);
    if (length > gf_string_t::PREFIX_LENGTH) {
        if (length <= gf_string_t::SHORT_STR_LENGTH) {
            memcpy(data, value + gf_string_t::PREFIX_LENGTH, length - gf_string_t::PREFIX_LENGTH);
        } else {
            memcpy(reinterpret_cast<char*>(overflowPtr), value, length);
        }
    }
}

string gf_string_t::getAsString(const gf_string_t& gf_string) {
    if (gf_string.len <= gf_string_t::SHORT_STR_LENGTH) {
        return string((char*)gf_string.prefix, gf_string.len);
    } else {
        return string(reinterpret_cast<char*>(gf_string.overflowPtr), gf_string.len);
    }
}

} // namespace common
} // namespace graphflow
