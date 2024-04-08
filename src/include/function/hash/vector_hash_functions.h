#pragma once

#include "common/vector/value_vector.h"
#include "function/function.h"

namespace kuzu {
namespace function {

struct UnaryHashFunctionExecutor {
    template<typename OPERAND_TYPE, typename RESULT_TYPE>
    static void execute(common::ValueVector& operand, common::ValueVector& result);
};

struct VectorHashFunction {
    static void computeHash(common::ValueVector* operand, common::ValueVector* result);

    static void combineHash(common::ValueVector* left, common::ValueVector* right,
        common::ValueVector* result);
};

struct MD5Function {
    static constexpr const char* name = "MD5";

    static function_set getFunctionSet();
};

struct SHA256Function {
    static constexpr const char* name = "SHA256";

    static function_set getFunctionSet();
};

struct HashFunction {
    static constexpr const char* name = "HASH";

    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
