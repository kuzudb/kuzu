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
    template<class OPERATION>
    static inline function_set getUnaryStrFunction(std::string funcName) {
        function_set functionSet;
        functionSet.emplace_back(std::make_unique<ScalarFunction>(funcName,
            std::vector<common::LogicalTypeID>{common::LogicalTypeID::STRING},
            common::LogicalTypeID::STRING,
            ScalarFunction::UnaryStringExecFunction<common::ku_string_t, common::ku_string_t,
                OPERATION>));
        return functionSet;
    }
};

struct ArrayExtractFunction {
    static constexpr const char* name = "ARRAY_EXTRACT";

    static function_set getFunctionSet();
};

struct ConcatFunction : public VectorStringFunction {
    static constexpr const char* name = "CONCAT";

    static void execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
        common::ValueVector& result, void* /*dataPtr*/);

    static function_set getFunctionSet();
};

struct ContainsFunction : public VectorStringFunction {
    static constexpr const char* name = "CONTAINS";

    static function_set getFunctionSet();
};

struct EndsWithFunction : public VectorStringFunction {
    static constexpr const char* name = "ENDS_WITH";

    static constexpr const char* alias = "SUFFIX";

    static function_set getFunctionSet();
};

struct LeftFunction : public VectorStringFunction {
    static constexpr const char* name = "LEFT";

    static function_set getFunctionSet();
};

struct LowerFunction : public VectorStringFunction {
    static constexpr const char* name = "LOWER";

    static constexpr const char* alias = "LCASE";

    static inline function_set getFunctionSet() { return getUnaryStrFunction<Lower>(name); }
};

struct LpadFunction : public VectorStringFunction {
    static constexpr const char* name = "LPAD";

    static function_set getFunctionSet();
};

struct LtrimFunction : public VectorStringFunction {
    static constexpr const char* name = "LTRIM";

    static inline function_set getFunctionSet() { return getUnaryStrFunction<Ltrim>(name); }
};

struct RepeatFunction : public VectorStringFunction {
    static constexpr const char* name = "REPEAT";

    static function_set getFunctionSet();
};

struct ReverseFunction : public VectorStringFunction {
    static constexpr const char* name = "REVERSE";

    static inline function_set getFunctionSet() { return getUnaryStrFunction<Reverse>(name); }
};

struct RightFunction : public VectorStringFunction {
    static constexpr const char* name = "RIGHT";

    static function_set getFunctionSet();
};

struct RpadFunction : public VectorStringFunction {
    static constexpr const char* name = "RPAD";

    static function_set getFunctionSet();
};

struct RtrimFunction : public VectorStringFunction {
    static constexpr const char* name = "RTRIM";

    static inline function_set getFunctionSet() { return getUnaryStrFunction<Rtrim>(name); }
};

struct StartsWithFunction : public VectorStringFunction {
    static constexpr const char* name = "STARTS_WITH";

    static constexpr const char* alias = "PREFIX";

    static function_set getFunctionSet();
};

struct SubStrFunction : public VectorStringFunction {
    static constexpr const char* name = "SUBSTR";

    static constexpr const char* alias = "SUBSTRING";

    static function_set getFunctionSet();
};

struct TrimFunction : public VectorStringFunction {
    static constexpr const char* name = "TRIM";

    static inline function_set getFunctionSet() { return getUnaryStrFunction<Trim>(name); }
};

struct UpperFunction : public VectorStringFunction {
    static constexpr const char* name = "UPPER";

    static constexpr const char* alias = "UCASE";

    static inline function_set getFunctionSet() { return getUnaryStrFunction<Upper>(name); }
};

struct RegexpFullMatchFunction : public VectorStringFunction {
    static constexpr const char* name = "REGEXP_FULL_MATCH";

    static function_set getFunctionSet();
};

struct RegexpMatchesFunction : public VectorStringFunction {
    static constexpr const char* name = "REGEXP_MATCHES";

    static function_set getFunctionSet();
};

struct RegexpReplaceFunction : public VectorStringFunction {
    static constexpr const char* name = "REGEXP_REPLACE";

    static function_set getFunctionSet();
};

struct RegexpExtractFunction : public VectorStringFunction {
    static constexpr const char* name = "REGEXP_EXTRACT";

    static function_set getFunctionSet();
};

struct RegexpExtractAllFunction : public VectorStringFunction {
    static constexpr const char* name = "REGEXP_EXTRACT_ALL";

    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(const binder::expression_vector& arguments,
        Function* function);
};

struct LevenshteinFunction : public VectorStringFunction {
    static constexpr const char* name = "LEVENSHTEIN";

    static function_set getFunctionSet();
};

struct InitcapFunction : public VectorStringFunction {
    static constexpr const char* name = "INITCAP";

    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
