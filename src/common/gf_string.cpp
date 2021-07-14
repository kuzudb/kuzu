#include "src/common/include/gf_string.h"

#include <cstring>

namespace graphflow {
namespace common {

void gf_string_t::setOverflowPtrInfo(const uint64_t& pageIdx, const uint16_t& pageOffset) {
    memcpy(&overflowPtr, &pageIdx, 6);
    memcpy(((uint8_t*)&overflowPtr) + 6, &pageOffset, 2);
}

void gf_string_t::getOverflowPtrInfo(uint64_t& pageIdx, uint16_t& pageOffset) const {
    pageIdx = 0;
    memcpy(&pageIdx, &overflowPtr, 6);
    memcpy(&pageOffset, ((uint8_t*)&overflowPtr) + 6, 2);
}

void gf_string_t::set(const string& value) {
    set(value.data(), value.length());
}

void gf_string_t::set(const char* value, uint64_t length) {
    this->len = length;
    if (length <= SHORT_STR_LENGTH) {
        memcpy(prefix, value, length);
    } else {
        memcpy(prefix, value, PREFIX_LENGTH);
        memcpy(reinterpret_cast<char*>(overflowPtr), value, length);
    }
}

void gf_string_t::set(const gf_string_t& value) {
    this->len = value.len;
    if (value.len <= SHORT_STR_LENGTH) {
        memcpy(prefix, value.prefix, value.len);
    } else {
        memcpy(prefix, value.prefix, PREFIX_LENGTH);
        memcpy(reinterpret_cast<char*>(overflowPtr), reinterpret_cast<char*>(value.overflowPtr),
            value.len);
    }
}

string gf_string_t::getAsShortString() const {
    return string((char*)prefix, len);
}

string gf_string_t::getAsString() const {
    if (len <= SHORT_STR_LENGTH) {
        return getAsShortString();
    } else {
        return string(reinterpret_cast<char*>(overflowPtr), len);
    }
}

} // namespace common
} // namespace graphflow
