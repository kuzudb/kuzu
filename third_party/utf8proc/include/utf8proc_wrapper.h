#pragma once

#include <cassert>
#include <cstring>
#include <string>

namespace graphflow {
namespace utf8proc {

enum class UnicodeType { INVALID, ASCII, UNICODE };
enum class UnicodeInvalidReason { BYTE_MISMATCH, INVALID_UNICODE };

class Utf8Proc {
public:
    static UnicodeType analyze(const char* s, size_t len,
        UnicodeInvalidReason* invalidReason = nullptr, size_t* invalidPos = nullptr);

    static char* normalize(const char* s, size_t len);

    static bool isValid(const char* s, size_t len);
};

} // namespace utf8proc
} // namespace graphflow
