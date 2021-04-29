#include "src/common/include/string.h"

#include <cassert>
#include <cstring>

namespace graphflow {
namespace common {

uint8_t* gf_string_t::resizeOverflowBufferIfNecessary(uint32_t capacity) {
    assert(capacity > SHORT_STR_LENGTH);
    uint8_t* buffer;
    if (len < capacity) {
        delete reinterpret_cast<uint8_t*>(overflowPtr);
        buffer = new uint8_t[capacity];
        overflowPtr = reinterpret_cast<uintptr_t>(buffer);
    } else {
        buffer = reinterpret_cast<uint8_t*>(overflowPtr);
    }
    return buffer;
}

gf_string_t& gf_string_t::operator=(const gf_string_t& other) {
    if (this == &other) {
        return *this;
    }

    if (other.isShort()) {
        deleteOverflowBufferIfAllocated();
        memcpy(prefix, other.prefix, other.len);
    } else {
        auto buffer = resizeOverflowBufferIfNecessary(other.len);
        memcpy(buffer, reinterpret_cast<uint8_t*>(other.overflowPtr), other.len);
        memcpy(prefix, buffer, gf_string_t::PREFIX_LENGTH);
    }
    len = other.len;
    return *this;
}

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
    len = str.size();
    auto cstr = str.c_str();
    auto prefixLen = len <= gf_string_t::PREFIX_LENGTH ? len : gf_string_t::PREFIX_LENGTH;
    memcpy(prefix, cstr, prefixLen);
    if (len > gf_string_t::PREFIX_LENGTH) {
        if (len <= gf_string_t::SHORT_STR_LENGTH) {
            memcpy(data, cstr + gf_string_t::PREFIX_LENGTH, len - gf_string_t::PREFIX_LENGTH);
        } else {
            auto cstrcopy = new char[len];
            memcpy(cstrcopy, cstr, len);
            overflowPtr = reinterpret_cast<uintptr_t>(cstrcopy);
        }
    }
}

string gf_string_t::getAsString(const gf_string_t& gf_string) {
    if (gf_string.len <= gf_string_t::SHORT_STR_LENGTH) {
        return std::string((char*)gf_string.prefix, gf_string.len);
    } else {
        return std::string(reinterpret_cast<char*>(gf_string.overflowPtr), gf_string.len);
    }
}

} // namespace common
} // namespace graphflow
