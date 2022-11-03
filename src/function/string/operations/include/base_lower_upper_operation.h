#pragma once

#include <cassert>
#include <cstring>

#include "third_party/utf8proc/include/utf8proc.h"

#include "src/common/types/include/gf_string.h"

using namespace graphflow::utf8proc;

namespace graphflow {
namespace function {
namespace operation {

struct BaseLowerUpperOperation {

    static inline void operation(
        gf_string_t& input, gf_string_t& result, ValueVector& resultValueVector, bool isUpper) {
        uint32_t resultLen = getResultLen((char*)input.getData(), input.len, isUpper);
        result.len = resultLen;
        if (resultLen <= gf_string_t::SHORT_STR_LENGTH) {
            convertCase((char*)result.prefix, input.len, (char*)input.getData(), isUpper);
        } else {
            result.overflowPtr = reinterpret_cast<uint64_t>(
                resultValueVector.getOverflowBuffer().allocateSpace(result.len));
            auto buffer = reinterpret_cast<char*>(result.overflowPtr);
            convertCase(buffer, input.len, (char*)input.getData(), isUpper);
            memcpy(result.prefix, buffer, gf_string_t::PREFIX_LENGTH);
        }
    }

    static uint32_t getResultLen(char* inputStr, uint32_t inputLen, bool isUpper) {
        uint32_t outputLength = 0;
        for (uint32_t i = 0; i < inputLen;) {
            // For UTF-8 characters, changing case can increase / decrease total byte length.
            // Eg.: 'ß' lower case -> 'SS' upper case [more bytes + more chars]
            if (inputStr[i] & 0x80) {
                int size = 0;
                int codepoint = utf8proc_codepoint(inputStr + i, size);
                int convertedCodepoint =
                    isUpper ? utf8proc_toupper(codepoint) : utf8proc_tolower(codepoint);
                int newSize = utf8proc_codepoint_length(convertedCodepoint);
                assert(newSize >= 0);
                outputLength += newSize;
                i += size;
            } else {
                outputLength++;
                i++;
            }
        }
        return outputLength;
    }

    static void convertCase(char* result, uint32_t len, char* input, bool toUpper) {
        for (auto i = 0u; i < len;) {
            if (input[i] & 0x80) {
                int size = 0, newSize = 0;
                int codepoint = utf8proc_codepoint(input + i, size);
                int convertedCodepoint =
                    toUpper ? utf8proc_toupper(codepoint) : utf8proc_tolower(codepoint);
                auto success = utf8proc_codepoint_to_utf8(convertedCodepoint, newSize, result);
                assert(success);
                result += newSize;
                i += size;
            } else {
                *result = toUpper ? toupper(input[i]) : tolower(input[i]);
                i++;
                result++;
            }
        }
    }
};
} // namespace operation
} // namespace function
} // namespace graphflow
