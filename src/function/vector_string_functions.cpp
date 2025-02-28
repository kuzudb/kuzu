#include "function/string/vector_string_functions.h"

#include "binder/expression/expression_util.h"
#include "common/exception/binder.h"
#include "function/string/functions/array_extract_function.h"
#include "function/string/functions/contains_function.h"
#include "function/string/functions/ends_with_function.h"
#include "function/string/functions/left_operation.h"
#include "function/string/functions/lpad_function.h"
#include "function/string/functions/regexp_extract_all_function.h"
#include "function/string/functions/regexp_extract_function.h"
#include "function/string/functions/regexp_full_match_function.h"
#include "function/string/functions/regexp_matches_function.h"
#include "function/string/functions/regexp_replace_function.h"
#include "function/string/functions/regexp_split_to_array_function.h"
#include "function/string/functions/repeat_function.h"
#include "function/string/functions/right_function.h"
#include "function/string/functions/rpad_function.h"
#include "function/string/functions/starts_with_function.h"
#include "function/string/functions/substr_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

void BaseLowerUpperFunction::operation(ku_string_t& input, ku_string_t& result,
    ValueVector& resultValueVector, bool isUpper) {
    uint32_t resultLen = getResultLen((char*)input.getData(), input.len, isUpper);
    result.len = resultLen;
    if (resultLen <= ku_string_t::SHORT_STR_LENGTH) {
        convertCase((char*)result.prefix, input.len, (char*)input.getData(), isUpper);
    } else {
        StringVector::reserveString(&resultValueVector, result, resultLen);
        auto buffer = reinterpret_cast<char*>(result.overflowPtr);
        convertCase(buffer, input.len, (char*)input.getData(), isUpper);
        memcpy(result.prefix, buffer, ku_string_t::PREFIX_LENGTH);
    }
}

void BaseStrOperation::operation(ku_string_t& input, ku_string_t& result,
    ValueVector& resultValueVector, uint32_t (*strOperation)(char* data, uint32_t len)) {
    if (input.len <= ku_string_t::SHORT_STR_LENGTH) {
        memcpy(result.prefix, input.prefix, input.len);
        result.len = strOperation((char*)result.prefix, input.len);
    } else {
        StringVector::reserveString(&resultValueVector, result, input.len);
        auto buffer = reinterpret_cast<char*>(result.overflowPtr);
        memcpy(buffer, input.getData(), input.len);
        result.len = strOperation(buffer, input.len);
        memcpy(result.prefix, buffer,
            result.len < ku_string_t::PREFIX_LENGTH ? result.len : ku_string_t::PREFIX_LENGTH);
    }
}

void Repeat::operation(ku_string_t& left, int64_t& right, ku_string_t& result,
    ValueVector& resultValueVector) {
    result.len = left.len * right;
    if (result.len <= ku_string_t::SHORT_STR_LENGTH) {
        repeatStr((char*)result.prefix, left.getAsString(), right);
    } else {
        StringVector::reserveString(&resultValueVector, result, result.len);
        auto buffer = reinterpret_cast<char*>(result.overflowPtr);
        repeatStr(buffer, left.getAsString(), right);
        memcpy(result.prefix, buffer, ku_string_t::PREFIX_LENGTH);
    }
}

void Reverse::operation(ku_string_t& input, ku_string_t& result, ValueVector& resultValueVector) {
    bool isAscii = true;
    std::string inputStr = input.getAsString();
    for (uint32_t i = 0; i < input.len; i++) {
        if (inputStr[i] & 0x80) {
            isAscii = false;
            break;
        }
    }
    if (isAscii) {
        BaseStrOperation::operation(input, result, resultValueVector, reverseStr);
    } else {
        result.len = input.len;
        if (result.len > ku_string_t::SHORT_STR_LENGTH) {
            StringVector::reserveString(&resultValueVector, result, input.len);
        }
        auto resultBuffer = result.len <= ku_string_t::SHORT_STR_LENGTH ?
                                reinterpret_cast<char*>(result.prefix) :
                                reinterpret_cast<char*>(result.overflowPtr);
        utf8proc::utf8proc_grapheme_callback(inputStr.c_str(), input.len,
            [&](size_t start, size_t end) {
                memcpy(resultBuffer + input.len - end, input.getData() + start, end - start);
                return true;
            });
        if (result.len > ku_string_t::SHORT_STR_LENGTH) {
            memcpy(result.prefix, resultBuffer, ku_string_t::PREFIX_LENGTH);
        }
    }
}

function_set ArrayExtractFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.emplace_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::INT64},
        LogicalTypeID::STRING,
        ScalarFunction::BinaryExecFunction<ku_string_t, int64_t, ku_string_t, ArrayExtract>));
    return functionSet;
}

void ConcatFunction::execFunc(const std::vector<std::shared_ptr<common::ValueVector>>& parameters,
    const std::vector<common::SelectionVector*>& parameterSelVectors, common::ValueVector& result,
    common::SelectionVector* resultSelVector, void* /*dataPtr*/) {
    result.resetAuxiliaryBuffer();
    for (auto selectedPos = 0u; selectedPos < resultSelVector->getSelSize(); ++selectedPos) {
        auto pos = (*resultSelVector)[selectedPos];
        auto strLen = 0u;
        for (auto i = 0u; i < parameters.size(); i++) {
            const auto& parameter = *parameters[i];
            const auto& parameterSelVector = parameterSelVectors[i];
            auto paramPos = (*parameterSelVector)[parameter.state->isFlat() ? 0 : selectedPos];
            strLen += parameter.getValue<ku_string_t>(paramPos).len;
        }
        auto& resultStr = result.getValue<ku_string_t>(pos);
        StringVector::reserveString(&result, resultStr, strLen);
        auto dstData = strLen <= ku_string_t::SHORT_STR_LENGTH ?
                           resultStr.prefix :
                           reinterpret_cast<uint8_t*>(resultStr.overflowPtr);
        for (auto i = 0u; i < parameters.size(); i++) {
            const auto& parameter = *parameters[i];
            const auto& parameterSelVector = parameterSelVectors[i];
            auto paramPos = (*parameterSelVector)[parameter.state->isFlat() ? 0 : selectedPos];
            auto srcStr = parameter.getValue<ku_string_t>(paramPos);
            memcpy(dstData, srcStr.getData(), srcStr.len);
            dstData += srcStr.len;
        }
        if (strLen > ku_string_t::SHORT_STR_LENGTH) {
            memcpy(resultStr.prefix, resultStr.getData(), ku_string_t::PREFIX_LENGTH);
        }
    }
}

function_set ConcatFunction::getFunctionSet() {
    function_set functionSet;
    auto function = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING}, LogicalTypeID::STRING, execFunc);
    function->isVarLength = true;
    functionSet.emplace_back(std::move(function));
    return functionSet;
}

function_set ContainsFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.emplace_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::BOOL,
        ScalarFunction::BinaryExecFunction<ku_string_t, ku_string_t, uint8_t, Contains>,
        ScalarFunction::BinarySelectFunction<ku_string_t, ku_string_t, Contains>));
    return functionSet;
}

function_set EndsWithFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.emplace_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::BOOL,
        ScalarFunction::BinaryExecFunction<ku_string_t, ku_string_t, uint8_t, EndsWith>,
        ScalarFunction::BinarySelectFunction<ku_string_t, ku_string_t, EndsWith>));
    return functionSet;
}

function_set LeftFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.emplace_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::INT64},
        LogicalTypeID::STRING,
        ScalarFunction::BinaryStringExecFunction<ku_string_t, int64_t, ku_string_t, Left>));
    return functionSet;
}

function_set LpadFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.emplace_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::INT64,
            LogicalTypeID::STRING},
        LogicalTypeID::STRING,
        ScalarFunction::TernaryStringExecFunction<ku_string_t, int64_t, ku_string_t, ku_string_t,
            Lpad>));
    return functionSet;
}

function_set RepeatFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.emplace_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::INT64},
        LogicalTypeID::STRING,
        ScalarFunction::BinaryStringExecFunction<ku_string_t, int64_t, ku_string_t, Repeat>));
    return functionSet;
}

function_set RightFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.emplace_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::INT64},
        LogicalTypeID::STRING,
        ScalarFunction::BinaryStringExecFunction<ku_string_t, int64_t, ku_string_t, Right>));
    return functionSet;
}

function_set RpadFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.emplace_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::INT64,
            LogicalTypeID::STRING},
        LogicalTypeID::STRING,
        ScalarFunction::TernaryStringExecFunction<ku_string_t, int64_t, ku_string_t, ku_string_t,
            Rpad>));
    return functionSet;
}

function_set StartsWithFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.emplace_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::BOOL,
        ScalarFunction::BinaryExecFunction<ku_string_t, ku_string_t, uint8_t, StartsWith>,
        ScalarFunction::BinarySelectFunction<ku_string_t, ku_string_t, StartsWith>));
    return functionSet;
}

function_set SubStrFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.emplace_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::INT64,
            LogicalTypeID::INT64},
        LogicalTypeID::STRING,
        ScalarFunction::TernaryStringExecFunction<ku_string_t, int64_t, int64_t, ku_string_t,
            SubStr>));
    return functionSet;
}

function_set RegexpFullMatchFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.emplace_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::BOOL,
        ScalarFunction::BinaryExecFunction<ku_string_t, ku_string_t, uint8_t, RegexpFullMatch>,
        ScalarFunction::BinarySelectFunction<ku_string_t, ku_string_t, RegexpFullMatch>));
    return functionSet;
}

function_set RegexpMatchesFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.emplace_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::BOOL,
        ScalarFunction::BinaryExecFunction<ku_string_t, ku_string_t, uint8_t, RegexpMatches>,
        ScalarFunction::BinarySelectFunction<ku_string_t, ku_string_t, RegexpMatches>));
    return functionSet;
}

std::unique_ptr<FunctionBindData> regexReplaceBindFunc(ScalarBindFuncInput input) {
    RegexpReplaceFunction::RegexReplaceOption replaceOption =
        RegexpReplaceFunction::RegexReplaceOption::FIRST_OCCUR;
    if (input.arguments.size() == 4) {
        auto option = input.arguments[3];
        binder::ExpressionUtil::validateExpressionType(*option, ExpressionType::LITERAL);
        binder::ExpressionUtil::validateDataType(*option, LogicalType::STRING());
        auto optionVal = binder::ExpressionUtil::getLiteralValue<std::string>(*option);
        if (optionVal == RegexpReplaceFunction::GLOBAL_REPLACE_OPTION) {
            replaceOption = RegexpReplaceFunction::RegexReplaceOption::GLOBAL;
        } else {
            throw common::BinderException{
                "regex_replace can only support global replace option: g."};
        }
    }
    KU_ASSERT(input.arguments.size() == 3 || input.arguments.size() == 4);
    return std::make_unique<RegexReplaceBindData>(
        binder::ExpressionUtil::getDataTypes(input.arguments), LogicalType::STRING(),
        replaceOption);
}

function_set RegexpReplaceFunction::getFunctionSet() {
    function_set functionSet;
    // Todo: Implement a function with modifiers
    //  regexp_replace(string, regex, replacement, modifiers)
    std::unique_ptr<ScalarFunction> func;
    func = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING,
            LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::STRING,
        ScalarFunction::TernaryRegexExecFunction<ku_string_t, ku_string_t, ku_string_t, ku_string_t,
            RegexpReplace>);
    func->bindFunc = regexReplaceBindFunc;
    functionSet.emplace_back(std::move(func));
    func = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING,
            LogicalTypeID::STRING},
        LogicalTypeID::STRING,
        ScalarFunction::TernaryRegexExecFunction<ku_string_t, ku_string_t, ku_string_t, ku_string_t,
            RegexpReplace>);
    func->bindFunc = regexReplaceBindFunc;
    functionSet.emplace_back(std::move(func));
    return functionSet;
}

function_set RegexpExtractFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.emplace_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::STRING,
        ScalarFunction::BinaryStringExecFunction<ku_string_t, ku_string_t, ku_string_t,
            RegexpExtract>));
    functionSet.emplace_back(make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING,
            LogicalTypeID::INT64},
        LogicalTypeID::STRING,
        ScalarFunction::TernaryStringExecFunction<ku_string_t, ku_string_t, int64_t, ku_string_t,
            RegexpExtract>));
    return functionSet;
}

static std::unique_ptr<FunctionBindData> bindFunc(const ScalarBindFuncInput /* input */
        &) {
    return std::make_unique<FunctionBindData>(LogicalType::LIST(LogicalType::STRING()));
}

function_set RegexpExtractAllFunction::getFunctionSet() {
    function_set functionSet;
    std::unique_ptr<ScalarFunction> func;
    func = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::LIST,
        ScalarFunction::BinaryStringExecFunction<ku_string_t, ku_string_t, list_entry_t,
            RegexpExtractAll>);
    func->bindFunc = bindFunc;
    functionSet.emplace_back(std::move(func));
    func = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING,
            LogicalTypeID::INT64},
        LogicalTypeID::LIST,
        ScalarFunction::TernaryStringExecFunction<ku_string_t, ku_string_t, int64_t, list_entry_t,
            RegexpExtractAll>);
    func->bindFunc = bindFunc;
    functionSet.emplace_back(std::move(func));
    return functionSet;
}

function_set RegexpSplitToArrayFunction::getFunctionSet() {
    function_set functionSet;
    auto func = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::LIST,
        ScalarFunction::BinaryStringExecFunction<ku_string_t, ku_string_t, list_entry_t,
            RegexpSplitToArray>);
    func->bindFunc = bindFunc;
    functionSet.emplace_back(std::move(func));
    return functionSet;
}

} // namespace function
} // namespace kuzu
