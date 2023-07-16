#include "function/string/vector_string_functions.h"

#include "function/string/functions/array_extract_function.h"
#include "function/string/functions/concat_function.h"
#include "function/string/functions/contains_function.h"
#include "function/string/functions/ends_with_function.h"
#include "function/string/functions/left_operation.h"
#include "function/string/functions/length_function.h"
#include "function/string/functions/lpad_function.h"
#include "function/string/functions/regexp_extract_all_function.h"
#include "function/string/functions/regexp_extract_function.h"
#include "function/string/functions/regexp_full_match_function.h"
#include "function/string/functions/regexp_matches_function.h"
#include "function/string/functions/regexp_replace_function.h"
#include "function/string/functions/repeat_function.h"
#include "function/string/functions/right_function.h"
#include "function/string/functions/rpad_function.h"
#include "function/string/functions/starts_with_function.h"
#include "function/string/functions/substr_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

vector_function_definitions ArrayExtractVectorFunction::getDefinitions() {
    vector_function_definitions definitions;
    definitions.emplace_back(make_unique<VectorFunctionDefinition>(ARRAY_EXTRACT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::INT64},
        LogicalTypeID::STRING, BinaryExecFunction<ku_string_t, int64_t, ku_string_t, ArrayExtract>,
        false /* isVarLength */));
    return definitions;
}

vector_function_definitions ConcatVectorFunction::getDefinitions() {
    vector_function_definitions definitions;
    definitions.emplace_back(make_unique<VectorFunctionDefinition>(CONCAT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::STRING,
        BinaryStringExecFunction<ku_string_t, ku_string_t, ku_string_t, Concat>,
        false /* isVarLength */));
    return definitions;
}

vector_function_definitions ContainsVectorFunction::getDefinitions() {
    vector_function_definitions definitions;
    definitions.emplace_back(make_unique<VectorFunctionDefinition>(CONTAINS_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::BOOL, BinaryExecFunction<ku_string_t, ku_string_t, uint8_t, Contains>,
        BinarySelectFunction<ku_string_t, ku_string_t, Contains>, false /* isVarLength */));
    return definitions;
}

vector_function_definitions EndsWithVectorFunction::getDefinitions() {
    vector_function_definitions definitions;
    definitions.emplace_back(make_unique<VectorFunctionDefinition>(ENDS_WITH_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::BOOL, BinaryExecFunction<ku_string_t, ku_string_t, uint8_t, EndsWith>,
        BinarySelectFunction<ku_string_t, ku_string_t, EndsWith>, false /* isVarLength */));
    return definitions;
}

vector_function_definitions LeftVectorFunction::getDefinitions() {
    vector_function_definitions definitions;
    definitions.emplace_back(make_unique<VectorFunctionDefinition>(LEFT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::INT64},
        LogicalTypeID::STRING, BinaryStringExecFunction<ku_string_t, int64_t, ku_string_t, Left>,
        false /* isVarLength */));
    return definitions;
}

vector_function_definitions LengthVectorFunction::getDefinitions() {
    vector_function_definitions definitions;
    definitions.emplace_back(make_unique<VectorFunctionDefinition>(LENGTH_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING}, LogicalTypeID::INT64,
        UnaryExecFunction<ku_string_t, int64_t, Length>, false /* isVarLength */));
    return definitions;
}

vector_function_definitions LpadVectorFunction::getDefinitions() {
    vector_function_definitions definitions;
    definitions.emplace_back(make_unique<VectorFunctionDefinition>(LPAD_FUNC_NAME,
        std::vector<LogicalTypeID>{
            LogicalTypeID::STRING, LogicalTypeID::INT64, LogicalTypeID::STRING},
        LogicalTypeID::STRING,
        TernaryStringExecFunction<ku_string_t, int64_t, ku_string_t, ku_string_t, Lpad>,
        false /* isVarLength */));
    return definitions;
}

vector_function_definitions RepeatVectorFunction::getDefinitions() {
    vector_function_definitions definitions;
    definitions.emplace_back(make_unique<VectorFunctionDefinition>(REPEAT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::INT64},
        LogicalTypeID::STRING, BinaryStringExecFunction<ku_string_t, int64_t, ku_string_t, Repeat>,
        false /* isVarLength */));
    return definitions;
}

vector_function_definitions RightVectorFunction::getDefinitions() {
    vector_function_definitions definitions;
    definitions.emplace_back(make_unique<VectorFunctionDefinition>(RIGHT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::INT64},
        LogicalTypeID::STRING, BinaryStringExecFunction<ku_string_t, int64_t, ku_string_t, Right>,
        false /* isVarLength */));
    return definitions;
}

vector_function_definitions RpadVectorFunction::getDefinitions() {
    vector_function_definitions definitions;
    definitions.emplace_back(make_unique<VectorFunctionDefinition>(RPAD_FUNC_NAME,
        std::vector<LogicalTypeID>{
            LogicalTypeID::STRING, LogicalTypeID::INT64, LogicalTypeID::STRING},
        LogicalTypeID::STRING,
        TernaryStringExecFunction<ku_string_t, int64_t, ku_string_t, ku_string_t, Rpad>,
        false /* isVarLength */));
    return definitions;
}

vector_function_definitions StartsWithVectorFunction::getDefinitions() {
    vector_function_definitions definitions;
    definitions.emplace_back(make_unique<VectorFunctionDefinition>(STARTS_WITH_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::BOOL, BinaryExecFunction<ku_string_t, ku_string_t, uint8_t, StartsWith>,
        BinarySelectFunction<ku_string_t, ku_string_t, StartsWith>, false /* isVarLength */));
    return definitions;
}

vector_function_definitions SubStrVectorFunction::getDefinitions() {
    vector_function_definitions definitions;
    definitions.emplace_back(make_unique<VectorFunctionDefinition>(SUBSTRING_FUNC_NAME,
        std::vector<LogicalTypeID>{
            LogicalTypeID::STRING, LogicalTypeID::INT64, LogicalTypeID::INT64},
        LogicalTypeID::STRING,
        TernaryStringExecFunction<ku_string_t, int64_t, int64_t, ku_string_t, SubStr>,
        false /* isVarLength */));
    return definitions;
}

vector_function_definitions RegexpFullMatchVectorFunction::getDefinitions() {
    vector_function_definitions definitions;
    definitions.emplace_back(make_unique<VectorFunctionDefinition>(REGEXP_FULL_MATCH_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::BOOL, BinaryExecFunction<ku_string_t, ku_string_t, uint8_t, RegexpFullMatch>,
        BinarySelectFunction<ku_string_t, ku_string_t, RegexpFullMatch>, false /* isVarLength */));
    return definitions;
}

vector_function_definitions RegexpMatchesVectorFunction::getDefinitions() {
    vector_function_definitions definitions;
    definitions.emplace_back(make_unique<VectorFunctionDefinition>(REGEXP_MATCHES_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::BOOL,
        VectorFunction::BinaryExecFunction<ku_string_t, ku_string_t, uint8_t, RegexpMatches>,
        VectorFunction::BinarySelectFunction<ku_string_t, ku_string_t, RegexpMatches>,
        false /* isVarLength */));
    return definitions;
}

vector_function_definitions RegexpReplaceVectorFunction::getDefinitions() {
    vector_function_definitions definitions;
    // Todo: Implement a function with modifiers
    //  regexp_replace(string, regex, replacement, modifiers)
    definitions.emplace_back(make_unique<VectorFunctionDefinition>(REGEXP_REPLACE_FUNC_NAME,
        std::vector<LogicalTypeID>{
            LogicalTypeID::STRING, LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::STRING,
        TernaryStringExecFunction<ku_string_t, ku_string_t, ku_string_t, ku_string_t,
            RegexpReplace>,
        false /* isVarLength */));
    return definitions;
}

vector_function_definitions RegexpExtractVectorFunction::getDefinitions() {
    vector_function_definitions definitions;
    definitions.emplace_back(make_unique<VectorFunctionDefinition>(REGEXP_EXTRACT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::STRING,
        BinaryStringExecFunction<ku_string_t, ku_string_t, ku_string_t, RegexpExtract>,
        false /* isVarLength */));
    definitions.emplace_back(make_unique<VectorFunctionDefinition>(REGEXP_EXTRACT_FUNC_NAME,
        std::vector<LogicalTypeID>{
            LogicalTypeID::STRING, LogicalTypeID::STRING, LogicalTypeID::INT64},
        LogicalTypeID::STRING,
        TernaryStringExecFunction<ku_string_t, ku_string_t, int64_t, ku_string_t, RegexpExtract>,
        false /* isVarLength */));
    return definitions;
}

vector_function_definitions RegexpExtractAllVectorFunction::getDefinitions() {
    vector_function_definitions definitions;
    definitions.emplace_back(make_unique<VectorFunctionDefinition>(REGEXP_EXTRACT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::VAR_LIST,
        BinaryStringExecFunction<ku_string_t, ku_string_t, list_entry_t, RegexpExtractAll>, nullptr,
        bindFunc, false /* isVarLength */));
    definitions.emplace_back(make_unique<VectorFunctionDefinition>(REGEXP_EXTRACT_FUNC_NAME,
        std::vector<LogicalTypeID>{
            LogicalTypeID::STRING, LogicalTypeID::STRING, LogicalTypeID::INT64},
        LogicalTypeID::VAR_LIST,
        TernaryStringExecFunction<ku_string_t, ku_string_t, int64_t, list_entry_t,
            RegexpExtractAll>,
        nullptr, bindFunc, false /* isVarLength */));
    return definitions;
}

std::unique_ptr<FunctionBindData> RegexpExtractAllVectorFunction::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    return std::make_unique<FunctionBindData>(LogicalType(LogicalTypeID::VAR_LIST,
        std::make_unique<VarListTypeInfo>(std::make_unique<LogicalType>(LogicalTypeID::STRING))));
}

} // namespace function
} // namespace kuzu
