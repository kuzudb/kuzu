#pragma once

#include <cassert>
#include <cstring>

#include "common/types/ku_string.h"
#include "common/vector/value_vector.h"
#include "utf8proc.h"

namespace kuzu {
namespace function {

struct BaseLowerUpperFunction {

    static inline void operation(common::ku_string_t& input, common::ku_string_t& result,
        common::ValueVector& resultValueVector, bool isUpper) {
        uint32_t resultLen = getResultLen((char*)input.getData(), input.len, isUpper);
        result.len = resultLen;
        if (resultLen <= common::ku_string_t::SHORT_STR_LENGTH) {
            convertCase((char*)result.prefix, input.len, (char*)input.getData(), isUpper);
        } else {
            result.overflowPtr = reinterpret_cast<uint64_t>(
                common::StringVector::getInMemOverflowBuffer(&resultValueVector)
                    ->allocateSpace(result.len));
            auto buffer = reinterpret_cast<char*>(result.overflowPtr);
            convertCase(buffer, input.len, (char*)input.getData(), isUpper);
            memcpy(result.prefix, buffer, common::ku_string_t::PREFIX_LENGTH);
        }
    }

private:
    static uint32_t getResultLen(char* inputStr, uint32_t inputLen, bool isUpper);
    static void convertCase(char* result, uint32_t len, char* input, bool toUpper);
};

} // namespace function
} // namespace kuzu
