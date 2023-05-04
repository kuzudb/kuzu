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

std::vector<std::unique_ptr<VectorOperationDefinition>>
ArrayExtractVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(ARRAY_EXTRACT_FUNC_NAME,
        std::vector<DataTypeID>{STRING, INT64}, STRING,
        BinaryExecFunction<ku_string_t, int64_t, ku_string_t, operation::ArrayExtract>,
        false /* isVarLength */));
    return definitions;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> ConcatVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(CONCAT_FUNC_NAME,
        std::vector<DataTypeID>{STRING, STRING}, STRING,
        BinaryStringExecFunction<ku_string_t, ku_string_t, ku_string_t, operation::Concat>,
        false /* isVarLength */));
    return definitions;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> ContainsVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(CONTAINS_FUNC_NAME,
        std::vector<DataTypeID>{STRING, STRING}, BOOL,
        BinaryExecFunction<ku_string_t, ku_string_t, uint8_t, operation::Contains>,
        BinarySelectFunction<ku_string_t, ku_string_t, operation::Contains>,
        false /* isVarLength */));
    return definitions;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> EndsWithVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(ENDS_WITH_FUNC_NAME,
        std::vector<DataTypeID>{STRING, STRING}, BOOL,
        BinaryExecFunction<ku_string_t, ku_string_t, uint8_t, operation::EndsWith>,
        BinarySelectFunction<ku_string_t, ku_string_t, operation::EndsWith>,
        false /* isVarLength */));
    return definitions;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> LeftVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(LEFT_FUNC_NAME,
        std::vector<DataTypeID>{STRING, INT64}, STRING,
        BinaryStringExecFunction<ku_string_t, int64_t, ku_string_t, operation::Left>,
        false /* isVarLength */));
    return definitions;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> LengthVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(LENGTH_FUNC_NAME,
        std::vector<DataTypeID>{STRING}, INT64,
        UnaryExecFunction<ku_string_t, int64_t, operation::Length>, false /* isVarLength */));
    return definitions;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> LpadVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(LPAD_FUNC_NAME,
        std::vector<DataTypeID>{STRING, INT64, STRING}, STRING,
        TernaryStringExecFunction<ku_string_t, int64_t, ku_string_t, ku_string_t, operation::Lpad>,
        false /* isVarLength */));
    return definitions;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> RepeatVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(REPEAT_FUNC_NAME,
        std::vector<DataTypeID>{STRING, INT64}, STRING,
        BinaryStringExecFunction<ku_string_t, int64_t, ku_string_t, operation::Repeat>,
        false /* isVarLength */));
    return definitions;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> RightVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(RIGHT_FUNC_NAME,
        std::vector<DataTypeID>{STRING, INT64}, STRING,
        BinaryStringExecFunction<ku_string_t, int64_t, ku_string_t, operation::Right>,
        false /* isVarLength */));
    return definitions;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> RpadVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(RPAD_FUNC_NAME,
        std::vector<DataTypeID>{STRING, INT64, STRING}, STRING,
        TernaryStringExecFunction<ku_string_t, int64_t, ku_string_t, ku_string_t, operation::Rpad>,
        false /* isVarLength */));
    return definitions;
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
StartsWithVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(STARTS_WITH_FUNC_NAME,
        std::vector<DataTypeID>{STRING, STRING}, BOOL,
        BinaryExecFunction<ku_string_t, ku_string_t, uint8_t, operation::StartsWith>,
        BinarySelectFunction<ku_string_t, ku_string_t, operation::StartsWith>,
        false /* isVarLength */));
    return definitions;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> SubStrVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(SUBSTRING_FUNC_NAME,
        std::vector<DataTypeID>{STRING, INT64, INT64}, STRING,
        TernaryStringExecFunction<ku_string_t, int64_t, int64_t, ku_string_t, operation::SubStr>,
        false /* isVarLength */));
    return definitions;
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
RegexpFullMatchVectorOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(REGEXP_FULL_MATCH_FUNC_NAME,
        std::vector<DataTypeID>{STRING, STRING}, BOOL,
        BinaryExecFunction<ku_string_t, ku_string_t, uint8_t, operation::RegexpFullMatch>,
        BinarySelectFunction<ku_string_t, ku_string_t, operation::RegexpFullMatch>,
        false /* isVarLength */));
    return definitions;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> RegexpMatchesOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(REGEXP_MATCHES_FUNC_NAME,
        std::vector<DataTypeID>{STRING, STRING}, BOOL,
        BinaryExecFunction<ku_string_t, ku_string_t, uint8_t, operation::RegexpMatches>,
        BinarySelectFunction<ku_string_t, ku_string_t, operation::RegexpMatches>,
        false /* isVarLength */));
    return definitions;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> RegexpReplaceOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    // Todo: Implement a function with modifiers
    //  regexp_replace(string, regex, replacement, modifiers)
    definitions.emplace_back(make_unique<VectorOperationDefinition>(REGEXP_REPLACE_FUNC_NAME,
        std::vector<DataTypeID>{STRING, STRING, STRING}, STRING,
        TernaryStringExecFunction<ku_string_t, ku_string_t, ku_string_t, ku_string_t,
            operation::RegexpReplace>,
        false /* isVarLength */));
    return definitions;
}

std::vector<std::unique_ptr<VectorOperationDefinition>> RegexpExtractOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(REGEXP_EXTRACT_FUNC_NAME,
        std::vector<DataTypeID>{STRING, STRING}, STRING,
        BinaryStringExecFunction<ku_string_t, ku_string_t, ku_string_t, operation::RegexpExtract>,
        false /* isVarLength */));
    definitions.emplace_back(make_unique<VectorOperationDefinition>(REGEXP_EXTRACT_FUNC_NAME,
        std::vector<DataTypeID>{STRING, STRING, INT64}, STRING,
        TernaryStringExecFunction<ku_string_t, ku_string_t, int64_t, ku_string_t,
            operation::RegexpExtract>,
        false /* isVarLength */));
    return definitions;
}

std::vector<std::unique_ptr<VectorOperationDefinition>>
RegexpExtractAllOperation::getDefinitions() {
    std::vector<std::unique_ptr<VectorOperationDefinition>> definitions;
    definitions.emplace_back(make_unique<VectorOperationDefinition>(REGEXP_EXTRACT_FUNC_NAME,
        std::vector<DataTypeID>{STRING, STRING}, VAR_LIST,
        BinaryStringExecFunction<ku_string_t, ku_string_t, list_entry_t,
            operation::RegexpExtractAll>,
        nullptr, bindFunc, false /* isVarLength */));
    definitions.emplace_back(make_unique<VectorOperationDefinition>(REGEXP_EXTRACT_FUNC_NAME,
        std::vector<DataTypeID>{STRING, STRING, INT64}, VAR_LIST,
        TernaryStringExecFunction<ku_string_t, ku_string_t, int64_t, list_entry_t,
            operation::RegexpExtractAll>,
        nullptr, bindFunc, false /* isVarLength */));
    return definitions;
}

std::unique_ptr<FunctionBindData> RegexpExtractAllOperation::bindFunc(
    const binder::expression_vector& arguments, FunctionDefinition* definition) {
    return std::make_unique<FunctionBindData>(DataType(std::make_unique<DataType>(STRING)));
}

} // namespace function
} // namespace kuzu
