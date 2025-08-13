#include "function/tokenize.h"

#include "binder/expression/expression_util.h"
#include "common/exception/binder.h"
#include "common/string_utils.h"
#include "common/types/ku_string.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "cppjieba/Jieba.hpp"
#include "expression_evaluator/expression_evaluator_utils.h"
#include "function/scalar_function.h"
#include "re2.h"

namespace kuzu {
namespace fts_extension {

using namespace function;
using namespace common;

struct JiebaBindData final : public FunctionBindData {
    std::shared_ptr<cppjieba::Jieba> jieba;

    JiebaBindData(common::logical_type_vec_t paramTypes, std::shared_ptr<cppjieba::Jieba> jieba)
        : FunctionBindData{std::move(paramTypes),
              common::LogicalType::LIST(common::LogicalType::STRING())},
          jieba{std::move(jieba)} {}

    std::unique_ptr<FunctionBindData> copy() const override {
        return std::make_unique<JiebaBindData>(copyVector(paramTypes), jieba);
    }
};

static void addTokensToVector(const std::vector<std::string>& tokens, list_entry_t& result,
    common::ValueVector& resultVector) {
    result = ListVector::addList(&resultVector, tokens.size());
    for (auto i = 0u; i < tokens.size(); i++) {
        ListVector::getDataVector(&resultVector)->setValue(result.offset + i, tokens[i]);
    }
}

struct JiebaTokenizer {
    static void operation(common::ku_string_t& text, common::ku_string_t& /*tokenizerName*/,
        common::ku_string_t& /*extraParam*/, list_entry_t& result,
        common::ValueVector& resultVector, void* dataPtr) {
        std::vector<std::string> tokens;
        auto bindData = reinterpret_cast<JiebaBindData*>(dataPtr);
        bindData->jieba->CutForSearch(text.getAsString(), tokens);
        addTokensToVector(tokens, result, resultVector);
    }
};

struct SimpleTokenizer {
    static void operation(common::ku_string_t& text, common::ku_string_t& /*tokenizerName*/,
        common::ku_string_t& /*extraParam*/, list_entry_t& result,
        common::ValueVector& resultVector, void* /*dataPtr*/) {
        auto tokens =
            StringUtils::split(text.getAsString(), " ", true /* ignoreEmptyStringParts */);
        addTokensToVector(tokens, result, resultVector);
    }
};

static std::unique_ptr<FunctionBindData> bindFunc(const ScalarBindFuncInput& input) {
    if (input.arguments[1]->expressionType != ExpressionType::LITERAL) {
        throw BinderException{"The tokenizer parameter must be a literal expression."};
    }
    if (input.arguments[2]->expressionType != ExpressionType::LITERAL) {
        throw BinderException{"The path to the jieba dict directory must be a literal expression."};
    }
    auto value = evaluator::ExpressionEvaluatorUtils::evaluateConstantExpression(input.arguments[1],
        input.context);
    auto tokenizer = common::StringUtils::getLower(value.getValue<std::string>());
    if (tokenizer == "jieba") {
        std::string dictDir = evaluator::ExpressionEvaluatorUtils::evaluateConstantExpression(
            input.arguments[2], input.context)
                                  .getValue<std::string>();
        std::string dict = dictDir + "/jieba.dict.utf8";
        std::string hmm = dictDir + "/hmm_model.utf8";
        std::string user = dictDir + "/user.dict.utf8"; // Contains custom AI/ML terms
        std::string idf = dictDir + "/idf.utf8";
        std::string stop = dictDir + "/stop_words.utf8";
        auto jieba = std::make_unique<cppjieba::Jieba>(dict, hmm, user, idf, stop);
        input.definition->ptrCast<ScalarFunction>()->execFunc =
            ScalarFunction::TernaryRegexExecFunction<ku_string_t, ku_string_t, ku_string_t,
                list_entry_t, JiebaTokenizer>;
        return std::make_unique<JiebaBindData>(
            binder::ExpressionUtil::getDataTypes(input.arguments), std::move(jieba));
    } else if (tokenizer == "simple" || tokenizer == "") {
        input.definition->ptrCast<ScalarFunction>()->execFunc =
            ScalarFunction::TernaryRegexExecFunction<ku_string_t, ku_string_t, ku_string_t,
                list_entry_t, SimpleTokenizer>;
        return FunctionBindData::getSimpleBindData(input.arguments,
            LogicalType::LIST(LogicalType::STRING()));
    } else {
        throw common::BinderException{
            "Unsupported tokenizer: " + tokenizer +
            ".\nSupported tokenizers: 'simple' (default), 'jieba' (advanced Chinese)"};
    }
}

function::function_set TokenizeFunction::getFunctionSet() {
    function_set result;
    auto function = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING,
            LogicalTypeID::STRING},
        LogicalTypeID::LIST);
    function->bindFunc = bindFunc;
    result.push_back(std::move(function));
    return result;
}

} // namespace fts_extension
} // namespace kuzu
