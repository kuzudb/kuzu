#pragma once

#include "function/scalar_function.h"
#include "function/string/functions/lower_function.h"
#include "function/string/functions/ltrim_function.h"
#include "function/string/functions/reverse_function.h"
#include "function/string/functions/rtrim_function.h"
#include "function/string/functions/trim_function.h"
#include "function/string/functions/upper_function.h"

namespace kuzu {
namespace function {

struct VectorStringFunction {

    template<typename A_TYPE, typename B_TYPE, typename C_TYPE, typename RESULT_TYPE, typename FUNC>
    static void TernaryStringExecFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result) {
        assert(params.size() == 3);
        TernaryFunctionExecutor::executeString<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE, FUNC>(
            *params[0], *params[1], *params[2], result);
    }

    template<typename LEFT_TYPE, typename RIGHT_TYPE, typename RESULT_TYPE, typename FUNC>
    static void BinaryStringExecFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result) {
        assert(params.size() == 2);
        BinaryFunctionExecutor::executeString<LEFT_TYPE, RIGHT_TYPE, RESULT_TYPE, FUNC>(
            *params[0], *params[1], result);
    }

    template<typename OPERAND_TYPE, typename RESULT_TYPE, typename FUNC>
    static void UnaryStringExecFunction(
        const std::vector<std::shared_ptr<common::ValueVector>>& params,
        common::ValueVector& result) {
        assert(params.size() == 1);
        UnaryFunctionExecutor::executeString<OPERAND_TYPE, RESULT_TYPE, FUNC>(*params[0], result);
    }

    template<class OPERATION>
    static inline function_set getUnaryStrFunction(std::string funcName) {
        function_set functionSet;
        functionSet.emplace_back(std::make_unique<ScalarFunction>(funcName,
            std::vector<common::LogicalTypeID>{common::LogicalTypeID::STRING},
            common::LogicalTypeID::STRING,
            UnaryStringExecFunction<common::ku_string_t, common::ku_string_t, OPERATION>,
            false /* isVarLength */));
        return functionSet;
    }
};

struct ArrayExtractFunction {
    static function_set getFunctionSet();
};

struct ConcatFunction : public VectorStringFunction {
    static function_set getFunctionSet();
};

struct ContainsFunction : public VectorStringFunction {
    static function_set getFunctionSet();
};

struct EndsWithFunction : public VectorStringFunction {
    static function_set getFunctionSet();
};

struct LeftFunction : public VectorStringFunction {
    static function_set getFunctionSet();
};

struct LowerFunction : public VectorStringFunction {
    static inline function_set getFunctionSet() {
        return getUnaryStrFunction<Lower>(common::LOWER_FUNC_NAME);
    }
};

struct LpadFunction : public VectorStringFunction {
    static function_set getFunctionSet();
};

struct LtrimFunction : public VectorStringFunction {
    static inline function_set getFunctionSet() {
        return getUnaryStrFunction<Ltrim>(common::LTRIM_FUNC_NAME);
    }
};

struct RepeatFunction : public VectorStringFunction {
    static function_set getFunctionSet();
};

struct ReverseFunction : public VectorStringFunction {
    static inline function_set getFunctionSet() {
        return getUnaryStrFunction<Reverse>(common::REVERSE_FUNC_NAME);
    }
};

struct RightFunction : public VectorStringFunction {
    static function_set getFunctionSet();
};

struct RpadFunction : public VectorStringFunction {
    static function_set getFunctionSet();
};

struct RtrimFunction : public VectorStringFunction {
    static inline function_set getFunctionSet() {
        return getUnaryStrFunction<Rtrim>(common::RTRIM_FUNC_NAME);
    }
};

struct StartsWithFunction : public VectorStringFunction {
    static function_set getFunctionSet();
};

struct SubStrFunction : public VectorStringFunction {
    static function_set getFunctionSet();
};

struct TrimFunction : public VectorStringFunction {
    static inline function_set getFunctionSet() {
        return getUnaryStrFunction<Trim>(common::TRIM_FUNC_NAME);
    }
};

struct UpperFunction : public VectorStringFunction {
    static inline function_set getFunctionSet() {
        return getUnaryStrFunction<Upper>(common::UPPER_FUNC_NAME);
    }
};

struct RegexpFullMatchFunction : public VectorStringFunction {
    static function_set getFunctionSet();
};

struct RegexpMatchesFunction : public VectorStringFunction {
    static function_set getFunctionSet();
};

struct RegexpReplaceFunction : public VectorStringFunction {
    static function_set getFunctionSet();
};

struct RegexpExtractFunction : public VectorStringFunction {
    static function_set getFunctionSet();
};

struct RegexpExtractAllFunction : public VectorStringFunction {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* function);
};

} // namespace function
} // namespace kuzu
