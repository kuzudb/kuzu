#pragma once

#include <cassert>
#include <cstring>

#include "base_str_operation.h"

#include "src/common/types/include/gf_string.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace function {
namespace operation {

struct Reverse {
public:
    static inline void operation(
        gf_string_t& input, gf_string_t& result, ValueVector& resultValueVector) {
        bool isAscii = true;
        string inputStr = input.getAsString();
        for (uint32_t i = 0; i < input.len; i++) {
            if (inputStr[i] & 0x80) {
                isAscii = false;
                break;
            }
        }
        if (isAscii) {
            BaseStrOperation::operation(input, result, resultValueVector, reverseStr);
        } else {
            result.len = input.len;
            if (result.len <= gf_string_t::SHORT_STR_LENGTH) {
                utf8proc_grapheme_callback(
                    inputStr.c_str(), input.len, [&](size_t start, size_t end) {
                        memcpy(
                            result.prefix + input.len - end, input.getData() + start, end - start);
                        return true;
                    });
            } else {
                result.overflowPtr = reinterpret_cast<uint64_t>(
                    resultValueVector.getOverflowBuffer().allocateSpace(input.len));
                auto buffer = reinterpret_cast<char*>(result.overflowPtr);
                utf8proc_grapheme_callback(
                    inputStr.c_str(), input.len, [&](size_t start, size_t end) {
                        memcpy(buffer + input.len - end, input.getData() + start, end - start);
                        return true;
                    });
                memcpy(result.prefix, buffer,
                    result.len < gf_string_t::PREFIX_LENGTH ? result.len :
                                                              gf_string_t::PREFIX_LENGTH);
            }
        }
    }

private:
    static uint32_t reverseStr(char* data, uint32_t len) {
        for (auto i = 0u; i < len / 2; i++) {
            swap(data[i], data[len - i - 1]);
        }
        return len;
    }
};

} // namespace operation
} // namespace function
} // namespace graphflow
