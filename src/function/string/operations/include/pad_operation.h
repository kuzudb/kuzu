#pragma once

#include <cassert>
#include <cstring>

#include "src/common/types/include/gf_string.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace function {
namespace operation {

struct PadOperation {
public:
    static inline void operation(gf_string_t& src, int64_t count, gf_string_t& characterToPad,
        gf_string_t& result, ValueVector& resultValueVector,
        void (*padOperation)(
            gf_string_t& src, int64_t count, gf_string_t& characterToPad, string& paddedResult)) {
        string paddedResult;
        padOperation(src, count, characterToPad, paddedResult);
        result.len = paddedResult.size();
        if (gf_string_t::isShortString(result.len)) {
            memcpy(result.prefix, paddedResult.data(), result.len);
        } else {
            result.overflowPtr = reinterpret_cast<uint64_t>(
                resultValueVector.getOverflowBuffer().allocateSpace(result.len));
            auto buffer = reinterpret_cast<char*>(result.overflowPtr);
            memcpy(buffer, paddedResult.data(), result.len);
            memcpy(result.prefix, buffer, gf_string_t::PREFIX_LENGTH);
        }
    }

    static pair<uint32_t, uint32_t> padCountChars(
        const uint32_t count, const char* data, const uint32_t size) {
        auto str = reinterpret_cast<const utf8proc_uint8_t*>(data);
        uint32_t byteCount = 0, charCount = 0;
        for (; charCount < count && byteCount < size; charCount++) {
            utf8proc_int32_t codepoint;
            auto bytes = utf8proc_iterate(str + byteCount, size - byteCount, &codepoint);
            byteCount += bytes;
        }
        return {byteCount, charCount};
    }

    static void insertPadding(uint32_t charCount, gf_string_t pad, string& result) {
        auto padData = pad.getData();
        auto padSize = pad.len;
        uint32_t padByteCount = 0;
        for (auto i = 0; i < charCount; i++) {
            if (padByteCount >= padSize) {
                result.insert(result.end(), (char*)padData, (char*)(padData + padByteCount));
                padByteCount = 0;
            }
            utf8proc_int32_t codepoint;
            auto bytes =
                utf8proc_iterate(padData + padByteCount, padSize - padByteCount, &codepoint);
            padByteCount += bytes;
        }
        result.insert(result.end(), (char*)padData, (char*)(padData + padByteCount));
    }
};

} // namespace operation
} // namespace function
} // namespace graphflow
