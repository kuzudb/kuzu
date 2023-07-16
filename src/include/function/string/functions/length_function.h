#pragma once

#include <cassert>
#include <cstring>

#include "common/types/ku_string.h"
#include "utf8proc.h"

namespace kuzu {
namespace function {

struct Length {
    static inline void operation(common::ku_string_t& input, int64_t& result) {
        auto totalByteLength = input.len;
        auto inputString = input.getAsString();
        for (auto i = 0; i < totalByteLength; i++) {
            if (inputString[i] & 0x80) {
                int64_t length = 0;
                // Use grapheme iterator to identify bytes of utf8 char and increment once for each
                // char.
                utf8proc::utf8proc_grapheme_callback(
                    inputString.c_str(), totalByteLength, [&](size_t start, size_t end) {
                        length++;
                        return true;
                    });
                result = length;
                return;
            }
        }
        result = totalByteLength;
    }
};

} // namespace function
} // namespace kuzu
