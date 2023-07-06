#include "function/string/functions/base_lower_upper_function.h"

using namespace kuzu::utf8proc;

namespace kuzu {
namespace function {

uint32_t BaseLowerUpperFunction::getResultLen(char* inputStr, uint32_t inputLen, bool isUpper) {
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

void BaseLowerUpperFunction::convertCase(char* result, uint32_t len, char* input, bool toUpper) {
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

} // namespace function
} // namespace kuzu
