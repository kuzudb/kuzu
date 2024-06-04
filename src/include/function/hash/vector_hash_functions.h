#pragma once

#include "common/vector/value_vector.h"
#include "function/function.h"

namespace kuzu {
namespace function {

struct UnaryHashFunctionExecutor {
    template<typename OPERAND_TYPE, typename RESULT_TYPE>
    static void execute(const common::ValueVector& operand,
        const common::SelectionVector& operandSelectVec, common::ValueVector& result,
        const common::SelectionVector& resultSelectVec);
};

struct BinaryHashFunctionExecutor {
    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    static void execute(const common::ValueVector& left, const common::SelectionVector& leftSelVec,
        const common::ValueVector& right, const common::SelectionVector& rightSelVec,
        common::ValueVector& result, const common::SelectionVector& resultSelVec);
};

struct VectorHashFunction {
    static void computeHash(const common::ValueVector& operand,
        const common::SelectionVector& operandSelectVec, common::ValueVector& result,
        const common::SelectionVector& resultSelectVec);

    static void combineHash(const common::ValueVector& left,
        const common::SelectionVector& leftSelVec, const common::ValueVector& right,
        const common::SelectionVector& rightSelVec, common::ValueVector& result,
        const common::SelectionVector& resultSelVec);
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
