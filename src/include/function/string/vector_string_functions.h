#pragma once

#include "function/string/functions/lower_function.h"
#include "function/string/functions/ltrim_function.h"
#include "function/string/functions/reverse_function.h"
#include "function/string/functions/rtrim_function.h"
#include "function/string/functions/trim_function.h"
#include "function/string/functions/upper_function.h"
#include "function/vector_functions.h"

namespace kuzu {
namespace function {

struct VectorStringFunction : public VectorFunction {

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
    static inline vector_function_definitions getUnaryStrFunctionDefintion(std::string funcName) {
        vector_function_definitions definitions;
        definitions.emplace_back(std::make_unique<VectorFunctionDefinition>(funcName,
            std::vector<common::LogicalTypeID>{common::LogicalTypeID::STRING},
            common::LogicalTypeID::STRING,
            UnaryStringExecFunction<common::ku_string_t, common::ku_string_t, OPERATION>,
            false /* isVarLength */));
        return definitions;
    }
};

struct ArrayExtractVectorFunction : public VectorStringFunction {
    static vector_function_definitions getDefinitions();
};

struct ConcatVectorFunction : public VectorStringFunction {
    static vector_function_definitions getDefinitions();
};

struct ContainsVectorFunction : public VectorStringFunction {
    static vector_function_definitions getDefinitions();
};

struct EndsWithVectorFunction : public VectorStringFunction {
    static vector_function_definitions getDefinitions();
};

struct LeftVectorFunction : public VectorStringFunction {
    static vector_function_definitions getDefinitions();
};

struct LengthVectorFunction : public VectorStringFunction {
    static vector_function_definitions getDefinitions();
};

struct LowerVectorFunction : public VectorStringFunction {
    static inline vector_function_definitions getDefinitions() {
        return getUnaryStrFunctionDefintion<Lower>(common::LOWER_FUNC_NAME);
    }
};

struct LpadVectorFunction : public VectorStringFunction {
    static vector_function_definitions getDefinitions();
};

struct LtrimVectorFunction : public VectorStringFunction {
    static inline vector_function_definitions getDefinitions() {
        return getUnaryStrFunctionDefintion<Ltrim>(common::LTRIM_FUNC_NAME);
    }
};

struct RepeatVectorFunction : public VectorStringFunction {
    static vector_function_definitions getDefinitions();
};

struct ReverseVectorFunction : public VectorStringFunction {
    static inline vector_function_definitions getDefinitions() {
        return getUnaryStrFunctionDefintion<Reverse>(common::REVERSE_FUNC_NAME);
    }
};

struct RightVectorFunction : public VectorStringFunction {
    static vector_function_definitions getDefinitions();
};

struct RpadVectorFunction : public VectorStringFunction {
    static vector_function_definitions getDefinitions();
};

struct RtrimVectorFunction : public VectorStringFunction {
    static inline vector_function_definitions getDefinitions() {
        return getUnaryStrFunctionDefintion<Rtrim>(common::RTRIM_FUNC_NAME);
    }
};

struct StartsWithVectorFunction : public VectorStringFunction {
    static vector_function_definitions getDefinitions();
};

struct SubStrVectorFunction : public VectorStringFunction {
    static vector_function_definitions getDefinitions();
};

struct TrimVectorFunction : public VectorStringFunction {
    static inline vector_function_definitions getDefinitions() {
        return getUnaryStrFunctionDefintion<Trim>(common::TRIM_FUNC_NAME);
    }
};

struct UpperVectorFunction : public VectorStringFunction {
    static inline vector_function_definitions getDefinitions() {
        return getUnaryStrFunctionDefintion<Upper>(common::UPPER_FUNC_NAME);
    }
};

struct RegexpFullMatchVectorFunction : public VectorStringFunction {
    static vector_function_definitions getDefinitions();
};

struct RegexpMatchesVectorFunction : public VectorStringFunction {
    static vector_function_definitions getDefinitions();
};

struct RegexpReplaceVectorFunction : public VectorStringFunction {
    static vector_function_definitions getDefinitions();
};

struct RegexpExtractVectorFunction : public VectorStringFunction {
    static vector_function_definitions getDefinitions();
};

struct RegexpExtractAllVectorFunction : public VectorStringFunction {
    static vector_function_definitions getDefinitions();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, FunctionDefinition* definition);
};

} // namespace function
} // namespace kuzu
