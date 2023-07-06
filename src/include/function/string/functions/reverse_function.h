#pragma once

#include <cassert>
#include <cstring>

#include "base_str_function.h"
#include "common/types/ku_string.h"

namespace kuzu {
namespace function {

struct Reverse {
public:
    static inline void operation(common::ku_string_t& input, common::ku_string_t& result,
        common::ValueVector& resultValueVector) {
        bool isAscii = true;
        std::string inputStr = input.getAsString();
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
            if (result.len > common::ku_string_t::SHORT_STR_LENGTH) {
                result.overflowPtr = reinterpret_cast<uint64_t>(
                    common::StringVector::getInMemOverflowBuffer(&resultValueVector)
                        ->allocateSpace(input.len));
            }
            auto resultBuffer = result.len <= common::ku_string_t::SHORT_STR_LENGTH ?
                                    reinterpret_cast<char*>(result.prefix) :
                                    reinterpret_cast<char*>(result.overflowPtr);
            utf8proc::utf8proc_grapheme_callback(
                inputStr.c_str(), input.len, [&](size_t start, size_t end) {
                    memcpy(resultBuffer + input.len - end, input.getData() + start, end - start);
                    return true;
                });
            if (result.len > common::ku_string_t::SHORT_STR_LENGTH) {
                memcpy(result.prefix, resultBuffer, common::ku_string_t::PREFIX_LENGTH);
            }
        }
    }

private:
    static uint32_t reverseStr(char* data, uint32_t len) {
        for (auto i = 0u; i < len / 2; i++) {
            std::swap(data[i], data[len - i - 1]);
        }
        return len;
    }
};

} // namespace function
} // namespace kuzu
