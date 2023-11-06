#pragma once

#include "function/scalar_function.h"

namespace kuzu {
namespace function {

struct OctetLengthFunctions {
    static function_set getFunctionSet();
};

struct EncodeFunctions {
    static function_set getFunctionSet();
};

struct DecodeFunctions {
    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
