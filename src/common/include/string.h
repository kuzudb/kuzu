#pragma once

#include <cstdint>
#include <string>

using namespace std;

namespace graphflow {
namespace common {

struct gf_string_t {

    static constexpr char EMPTY_STRING = 0;

    static const uint64_t PREFIX_LENGTH = 4;
    static const uint64_t STR_LENGTH_PLUS_PREFIX_LENGTH = sizeof(uint32_t) + PREFIX_LENGTH;
    static const uint64_t INLINED_SUFFIX_LENGTH = 8;
    static const uint64_t SHORT_STR_LENGTH = PREFIX_LENGTH + INLINED_SUFFIX_LENGTH;

    uint32_t len = 0;
    uint8_t prefix[PREFIX_LENGTH];
    union {
        uint8_t data[INLINED_SUFFIX_LENGTH];
        uint64_t overflowPtr;
    };

    ~gf_string_t() { deleteOverflowBufferIfAllocated(); }

    gf_string_t& operator=(const gf_string_t& other);

    void deleteOverflowBufferIfAllocated() {
        if (!isShort()) {
            delete reinterpret_cast<uint8_t*>(overflowPtr);
        }
    }

    uint8_t* resizeOverflowBufferIfNecessary(uint32_t capacity);

    void setOverflowPtrInfo(const uint64_t& pageIdx, const uint16_t& pageOffset);
    void getOverflowPtrInfo(uint64_t& pageIdx, uint16_t& pageOffset);

    inline bool isShort() const { return len <= SHORT_STR_LENGTH; }

    inline const uint8_t* getData() const {
        return len <= SHORT_STR_LENGTH ? prefix : reinterpret_cast<uint8_t*>(overflowPtr);
    }

    void set(const string& str);

    static string getAsString(const gf_string_t& gf_string);
};

} // namespace common
} // namespace graphflow
