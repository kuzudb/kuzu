#pragma once

#include "common/api.h"
#include "common/types/ku_string.h"
#include "common/vector/value_vector.h"

namespace kuzu {
namespace function {

struct BaseLowerUpperFunction {

    KUZU_API static void operation(common::ku_string_t& input, common::ku_string_t& result,
        common::ValueVector& resultValueVector, bool isUpper);

private:
    static uint32_t getResultLen(char* inputStr, uint32_t inputLen, bool isUpper);
    static void convertCase(char* result, uint32_t len, char* input, bool toUpper);
};

} // namespace function
} // namespace kuzu
