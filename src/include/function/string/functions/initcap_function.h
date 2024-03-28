#pragma once

#include "base_lower_upper_function.h"
#include "lower_function.h"

namespace kuzu {
namespace function {

struct Initcap {
    static void operation(common::ku_string_t& operand, common::ku_string_t& result,
        common::ValueVector& resultVector) {
        Lower::operation(operand, result, resultVector);
        BaseLowerUpperFunction::convertCharCase(reinterpret_cast<char*>(result.getDataUnsafe()),
            reinterpret_cast<const char*>(result.getData()), 0 /* charPos */, true /* toUpper */);
    }
};

} // namespace function
} // namespace kuzu
