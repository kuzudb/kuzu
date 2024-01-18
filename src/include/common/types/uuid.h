#pragma once
// pragma once doesn't appear to work properly on MSVC for this file
#ifndef KUZU_COMMON_TYPES_UUID
#define KUZU_COMMON_TYPES_UUID

#include "common/types/int128_t.h"

namespace kuzu {
namespace common {

class RandomEngine;

struct uuid_t {
    constexpr static uint8_t UUID_STRING_LENGTH = 36;
    constexpr static char HEX_DIGITS[] = "0123456789abcdef";
    static void byteToHex(char byteVal, char* buf, uint64_t& pos);
    static unsigned char hex2Char(char ch);
    static inline bool isHex(char ch) {
        return (ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F');
    }

    // Convert a uuid string to a int128_t object
    static bool fromString(std::string str, int128_t& result);

    static inline int128_t fromString(std::string str) {
        int128_t result;
        fromString(str, result);
        return result;
    }

    static inline int128_t fromCString(const char* str, uint64_t len) {
        return fromString(std::string(str, len));
    }

    // Convert a int128_t object to a uuid style string
    static void toString(int128_t input, char* buf);

    static inline std::string toString(int128_t input) {
        char buff[UUID_STRING_LENGTH];
        toString(input, buff);
        return std::string(buff, UUID_STRING_LENGTH);
    }
    inline std::string toString() const { return toString(value); }

    int128_t value;

    // generate a random uuid object
    static uuid_t generateRandomUUID(RandomEngine* engine);
};

} // namespace common
} // namespace kuzu
#endif
