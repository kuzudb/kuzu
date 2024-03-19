#pragma once

#include "function/function.h"

namespace kuzu {
namespace function {

struct ArrayValueFunction {
    static constexpr const char* name = "ARRAY_VALUE";

    static function_set getFunctionSet();
};

struct ArrayCrossProductFunction {
    static constexpr const char* name = "ARRAY_CROSS_PRODUCT";

    static function_set getFunctionSet();
};

struct ArrayCosineSimilarityFunction {
    static constexpr const char* name = "ARRAY_COSINE_SIMILARITY";

    static function_set getFunctionSet();
};

struct ArrayDistanceFunction {
    static constexpr const char* name = "ARRAY_DISTANCE";

    static function_set getFunctionSet();
};

struct ArrayInnerProductFunction {
    static constexpr const char* name = "ARRAY_INNER_PRODUCT";

    static function_set getFunctionSet();
};

struct ArrayDotProductFunction {
    static constexpr const char* name = "ARRAY_DOT_PRODUCT";

    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
