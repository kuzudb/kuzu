#pragma once

#include <string>

namespace kuzu {
namespace common {

struct ku_string_t {

    static const uint64_t PREFIX_LENGTH = 4;
    static const uint64_t INLINED_SUFFIX_LENGTH = 8;
    static const uint64_t SHORT_STR_LENGTH = PREFIX_LENGTH + INLINED_SUFFIX_LENGTH;

    uint32_t len;
    uint8_t prefix[PREFIX_LENGTH];
    union {
        uint8_t data[INLINED_SUFFIX_LENGTH];
        uint64_t overflowPtr;
    };

    ku_string_t() : len{0}, overflowPtr{0} {}

    static bool isShortString(uint32_t len) { return len <= SHORT_STR_LENGTH; }

    inline const uint8_t* getData() const {
        return isShortString(len) ? prefix : reinterpret_cast<uint8_t*>(overflowPtr);
    }

    // These functions do *NOT* allocate/resize the overflow buffer, it only copies the content and
    // set the length.
    void set(const std::string& value);
    void set(const char* value, uint64_t length);
    void set(const ku_string_t& value);

    std::string getAsShortString() const;
    std::string getAsString() const;

    bool operator==(const ku_string_t& rhs) const;

    inline bool operator!=(const ku_string_t& rhs) const { return !(*this == rhs); }

    bool operator>(const ku_string_t& rhs) const;

    inline bool operator>=(const ku_string_t& rhs) const { return (*this > rhs) || (*this == rhs); }

    inline bool operator<(const ku_string_t& rhs) const { return !(*this >= rhs); }

    inline bool operator<=(const ku_string_t& rhs) const { return !(*this > rhs); }
};

} // namespace common
} // namespace kuzu
