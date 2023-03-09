#include "common/types/ku_string.h"

#include <cstring>

namespace kuzu {
namespace common {

void ku_string_t::set(const std::string& value) {
    set(value.data(), value.length());
}

void ku_string_t::set(const char* value, uint64_t length) {
    this->len = length;
    if (length <= SHORT_STR_LENGTH) {
        memcpy(prefix, value, length);
    } else {
        memcpy(prefix, value, PREFIX_LENGTH);
        memcpy(reinterpret_cast<char*>(overflowPtr), value, length);
    }
}

void ku_string_t::set(const ku_string_t& value) {
    this->len = value.len;
    if (value.len <= SHORT_STR_LENGTH) {
        memcpy(prefix, value.prefix, value.len);
    } else {
        memcpy(prefix, value.prefix, PREFIX_LENGTH);
        memcpy(reinterpret_cast<char*>(overflowPtr), reinterpret_cast<char*>(value.overflowPtr),
            value.len);
    }
}

std::string ku_string_t::getAsShortString() const {
    return std::string((char*)prefix, len);
}

std::string ku_string_t::getAsString() const {
    if (len <= SHORT_STR_LENGTH) {
        return getAsShortString();
    } else {
        return std::string(reinterpret_cast<char*>(overflowPtr), len);
    }
}

bool ku_string_t::operator==(const ku_string_t& rhs) const {
    // First compare the length and prefix of the strings.
    auto numBytesOfLenAndPrefix =
        sizeof(uint32_t) +
        std::min((uint64_t)len, static_cast<uint64_t>(ku_string_t::PREFIX_LENGTH));
    if (!memcmp(this, &rhs, numBytesOfLenAndPrefix)) {
        // If length and prefix of a and b are equal, we compare the overflow buffer.
        return !memcmp(getData(), rhs.getData(), len);
    }
    return false;
}

bool ku_string_t::operator>(const ku_string_t& rhs) const {
    // Compare ku_string_t up to the shared length.
    // If there is a tie, we just need to compare the std::string lengths.
    auto sharedLen = std::min(len, rhs.len);
    auto memcmpResult = memcmp(prefix, rhs.prefix,
        sharedLen <= ku_string_t::PREFIX_LENGTH ? sharedLen : ku_string_t::PREFIX_LENGTH);
    if (memcmpResult == 0 && len > ku_string_t::PREFIX_LENGTH) {
        memcmpResult = memcmp(getData(), rhs.getData(), sharedLen);
    }
    if (memcmpResult == 0) {
        return len > rhs.len;
    }
    return memcmpResult > 0;
}

} // namespace common
} // namespace kuzu
