#include "function/string/vector_string_operations.h"

#include "function/string/operations/array_extract_operation.h"
#include "function/string/operations/concat_operation.h"
#include "function/string/operations/contains_operation.h"
#include "function/string/operations/ends_with_operation.h"
#include "function/string/operations/left_operation.h"
#include "function/string/operations/length_operation.h"
#include "function/string/operations/lpad_operation.h"
#include "function/string/operations/regexp_extract_all_operation.h"
#include "function/string/operations/regexp_extract_operation.h"
#include "function/string/operations/regexp_full_match_operation.h"
#include "function/string/operations/regexp_matches_operation.h"
#include "function/string/operations/regexp_replace_operation.h"
#include "function/string/operations/repeat_operation.h"
#include "function/string/operations/right_operation.h"
#include "function/string/operations/rpad_operation.h"
#include "function/string/operations/starts_with_operation.h"
#include "function/string/operations/substr_operation.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

vector_operation_definitions ArrayExtractVectorOperation::getDefinitions() {
    vector_operation_definitions definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(ARRAY_EXTRACT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::INT64},
        LogicalTypeID::STRING,
        BinaryExecFunction<ku_string_t, int64_t, ku_string_t, operation::ArrayExtract>,
        false /* isVarLength */));
    return definitions;
}

vector_operation_definitions ConcatVectorOperation::getDefinitions() {
    vector_operation_definitions definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(CONCAT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::STRING,
        BinaryStringExecFunction<ku_string_t, ku_string_t, ku_string_t, operation::Concat>,
        false /* isVarLength */));
    return definitions;
}

vector_operation_definitions ContainsVectorOperation::getDefinitions() {
    vector_operation_definitions definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(CONTAINS_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::BOOL,
        BinaryExecFunction<ku_string_t, ku_string_t, uint8_t, operation::Contains>,
        BinarySelectFunction<ku_string_t, ku_string_t, operation::Contains>,
        false /* isVarLength */));
    return definitions;
}

vector_operation_definitions EndsWithVectorOperation::getDefinitions() {
    vector_operation_definitions definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(ENDS_WITH_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::BOOL,
        BinaryExecFunction<ku_string_t, ku_string_t, uint8_t, operation::EndsWith>,
        BinarySelectFunction<ku_string_t, ku_string_t, operation::EndsWith>,
        false /* isVarLength */));
    return definitions;
}

vector_operation_definitions LeftVectorOperation::getDefinitions() {
    vector_operation_definitions definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(LEFT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::INT64},
        LogicalTypeID::STRING,
        BinaryStringExecFunction<ku_string_t, int64_t, ku_string_t, operation::Left>,
        false /* isVarLength */));
    return definitions;
}

vector_operation_definitions LengthVectorOperation::getDefinitions() {
    vector_operation_definitions definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(LENGTH_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING}, LogicalTypeID::INT64,
        UnaryExecFunction<ku_string_t, int64_t, operation::Length>, false /* isVarLength */));
    return definitions;
}

vector_operation_definitions LpadVectorOperation::getDefinitions() {
    vector_operation_definitions definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(LPAD_FUNC_NAME,
        std::vector<LogicalTypeID>{
            LogicalTypeID::STRING, LogicalTypeID::INT64, LogicalTypeID::STRING},
        LogicalTypeID::STRING,
        TernaryStringExecFunction<ku_string_t, int64_t, ku_string_t, ku_string_t, operation::Lpad>,
        false /* isVarLength */));
    return definitions;
}

vector_operation_definitions RepeatVectorOperation::getDefinitions() {
    vector_operation_definitions definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(REPEAT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::INT64},
        LogicalTypeID::STRING,
        BinaryStringExecFunction<ku_string_t, int64_t, ku_string_t, operation::Repeat>,
        false /* isVarLength */));
    return definitions;
}

vector_operation_definitions RightVectorOperation::getDefinitions() {
    vector_operation_definitions definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(RIGHT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::INT64},
        LogicalTypeID::STRING,
        BinaryStringExecFunction<ku_string_t, int64_t, ku_string_t, operation::Right>,
        false /* isVarLength */));
    return definitions;
}

vector_operation_definitions RpadVectorOperation::getDefinitions() {
    vector_operation_definitions definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(RPAD_FUNC_NAME,
        std::vector<LogicalTypeID>{
            LogicalTypeID::STRING, LogicalTypeID::INT64, LogicalTypeID::STRING},
        LogicalTypeID::STRING,
        TernaryStringExecFunction<ku_string_t, int64_t, ku_string_t, ku_string_t, operation::Rpad>,
        false /* isVarLength */));
    return definitions;
}

vector_operation_definitions StartsWithVectorOperation::getDefinitions() {
    vector_operation_definitions definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(STARTS_WITH_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::BOOL,
        BinaryExecFunction<ku_string_t, ku_string_t, uint8_t, operation::StartsWith>,
        BinarySelectFunction<ku_string_t, ku_string_t, operation::StartsWith>,
        false /* isVarLength */));
    return definitions;
}

vector_operation_definitions SubStrVectorOperation::getDefinitions() {
    vector_operation_definitions definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(SUBSTRING_FUNC_NAME,
        std::vector<LogicalTypeID>{
            LogicalTypeID::STRING, LogicalTypeID::INT64, LogicalTypeID::INT64},
        LogicalTypeID::STRING,
        TernaryStringExecFunction<ku_string_t, int64_t, int64_t, ku_string_t, operation::SubStr>,
        false /* isVarLength */));
    return definitions;
}

vector_operation_definitions RegexpFullMatchVectorOperation::getDefinitions() {
    vector_operation_definitions definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(REGEXP_FULL_MATCH_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::BOOL,
        BinaryExecFunction<ku_string_t, ku_string_t, uint8_t, operation::RegexpFullMatch>,
        BinarySelectFunction<ku_string_t, ku_string_t, operation::RegexpFullMatch>,
        false /* isVarLength */));
    return definitions;
}

vector_operation_definitions RegexpMatchesOperation::getDefinitions() {
    vector_operation_definitions definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(REGEXP_MATCHES_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::BOOL,
        BinaryExecFunction<ku_string_t, ku_string_t, uint8_t, operation::RegexpMatches>,
        BinarySelectFunction<ku_string_t, ku_string_t, operation::RegexpMatches>,
        false /* isVarLength */));
    return definitions;
}

vector_operation_definitions RegexpReplaceOperation::getDefinitions() {
    vector_operation_definitions definitions;
    // Todo: Implement a function with modifiers
    //  regexp_replace(string, regex, replacement, modifiers)
    definitions.emplace_back(make_unique<VectorOperationDefinition>(REGEXP_REPLACE_FUNC_NAME,
        std::vector<LogicalTypeID>{
            LogicalTypeID::STRING, LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::STRING,
        TernaryStringExecFunction<ku_string_t, ku_string_t, ku_string_t, ku_string_t,
            operation::RegexpReplace>,
        false /* isVarLength */));
    return definitions;
}

vector_operation_definitions RegexpExtractOperation::getDefinitions() {
    vector_operation_definitions definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(REGEXP_EXTRACT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::STRING,
        BinaryStringExecFunction<ku_string_t, ku_string_t, ku_string_t, operation::RegexpExtract>,
        false /* isVarLength */));
    definitions.emplace_back(make_unique<VectorOperationDefinition>(REGEXP_EXTRACT_FUNC_NAME,
        std::vector<LogicalTypeID>{
            LogicalTypeID::STRING, LogicalTypeID::STRING, LogicalTypeID::INT64},
        LogicalTypeID::STRING,
        TernaryStringExecFunction<ku_string_t, ku_string_t, int64_t, ku_string_t,
            operation::RegexpExtract>,
        false /* isVarLength */));
    return definitions;
}

vector_operation_definitions RegexpExtractAllOperation::getDefinitions() {
    vector_operation_definitions definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(REGEXP_EXTRACT_FUNC_NAME,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::VAR_LIST,
        BinaryStringExecFunction<ku_string_t, ku_string_t, list_entry_t,
            operation::RegexpExtractAll>,
        nullptr, bindFunc, false /* isVarLength */));
    definitions.emplace_back(make_unique<VectorOperationDefinition>(REGEXP_EXTRACT_FUNC_NAME,
        std::vector<LogicalTypeID>{
            LogicalTypeID::STRING, LogicalTypeID::STRING, LogicalTypeID::INT64},
        LogicalTypeID::VAR_LIST,
        TernaryStringExecFunction<ku_string_t, ku_string_t, int64_t, list_entry_t,
            operation::RegexpExtractAll>,
        nullptr, bindFunc, false /* isVarLength */));
    return definitions;
}

std::unique_ptr<FunctionBindData> RegexpExtractAllOperation::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    return std::make_unique<FunctionBindData>(LogicalType(LogicalTypeID::VAR_LIST,
        std::make_unique<VarListTypeInfo>(std::make_unique<LogicalType>(LogicalTypeID::STRING))));
}

} // namespace function
} // namespace kuzu
