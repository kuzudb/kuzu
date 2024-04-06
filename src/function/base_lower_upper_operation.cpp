#include "common/exception/runtime.h"
#include "common/string_format.h"
#include "function/string/functions/base_lower_upper_function.h"
#include "utf8proc.h"

using namespace kuzu::common;
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
            if (codepoint < 0) {
                // LCOV_EXCL_START
                // TODO(Xiyang): We shouldn't allow invalid UTF-8 to enter a string column.
                std::string funcName = isUpper ? "UPPER" : "LOWER";
                throw RuntimeException(
                    common::stringFormat("Failed calling {}: Invalid UTF-8.", funcName));
                // LCOV_EXCL_STOP
            }
            int convertedCodepoint =
                isUpper ? utf8proc_toupper(codepoint) : utf8proc_tolower(codepoint);
            int newSize = utf8proc_codepoint_length(convertedCodepoint);
            KU_ASSERT(newSize >= 0);
            outputLength += newSize;
            i += size;
        } else {
            outputLength++;
            i++;
        }
    }
    return outputLength;
}

uint64_t BaseLowerUpperFunction::convertCharCase(char* result, const char* input, int32_t charPos,
    bool toUpper) {
    if (input[charPos] & 0x80) {
        int size = 0u, newSize = 0u;
        auto codepoint = utf8proc_codepoint(input + charPos, size);
        KU_ASSERT(codepoint >= 0); // Validity ensured by getResultLen.
        int convertedCodepoint =
            toUpper ? utf8proc_toupper(codepoint) : utf8proc_tolower(codepoint);
        utf8proc_codepoint_to_utf8(convertedCodepoint, newSize, result);
        return size;
    } else {
        *result = toUpper ? toupper(input[charPos]) : tolower(input[charPos]);
        return 1;
    }
}

void BaseLowerUpperFunction::convertCase(char* result, uint32_t len, char* input, bool toUpper) {
    for (auto i = 0u; i < len;) {
        auto charWidth = convertCharCase(result, input, i, toUpper);
        i += charWidth;
        result += charWidth;
    }
}

} // namespace function
} // namespace kuzu
