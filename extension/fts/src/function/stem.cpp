#include "function/stem.h"

#include "binder/expression/expression_util.h"
#include "common/exception/binder.h"
#include "common/exception/runtime.h"
#include "common/string_utils.h"
#include "common/types/ku_string.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "expression_evaluator/expression_evaluator_utils.h"
#include "function/scalar_function.h"
#include "libstemmer.h"

namespace kuzu {
namespace fts_extension {

using namespace function;
using namespace common;

std::string getStemmerList() {
    std::string result;
    auto stemmerLst = sb_stemmer_list();
    for (auto i = 0u; i < 26; ++i) {
        result += stemmerLst[i];
        result += ", ";
    }
    result += stemmerLst[26];
    return result;
}

struct Stem {
    static void operation(common::ku_string_t& word, common::ku_string_t& stemmer,
        common::ku_string_t& result, common::ValueVector& resultVector);
};

void Stem::operation(common::ku_string_t& word, common::ku_string_t& stemmer,
    common::ku_string_t& result, common::ValueVector& resultVector) {
    if (stemmer.getAsString() == "none") {
        StringVector::addString(&resultVector, result, word);
        return;
    }

    struct sb_stemmer* sbStemmer =
        sb_stemmer_new(reinterpret_cast<const char*>(stemmer.getData()), "UTF_8");
    if (sbStemmer == nullptr) {
        throw common::RuntimeException(
            common::stringFormat("Unrecognized stemmer '{}'. Supported stemmers are: ['{}'], or "
                                 "use 'none' for no stemming.",
                stemmer.getAsString(), getStemmerList()));
    }

    auto stemData =
        sb_stemmer_stem(sbStemmer, reinterpret_cast<const sb_symbol*>(word.getData()), word.len);
    common::StringVector::addString(&resultVector, result, reinterpret_cast<const char*>(stemData),
        sb_stemmer_length(sbStemmer));
    sb_stemmer_delete(sbStemmer);
}

struct StemStaticStemmer {
    static void operation(common::ku_string_t& word, common::ku_string_t& /*stemmer*/,
        common::ku_string_t& result, common::ValueVector& /*leftValueVector*/,
        common::ValueVector& /*rightValueVector*/, common::ValueVector& resultVector,
        void* dataPtr);
};

struct StemWithoutStemmer {
    static void operation(common::ku_string_t& word, common::ku_string_t& /*stemmer*/,
        common::ku_string_t& result, common::ValueVector& /*leftValueVector*/,
        common::ValueVector& /*rightValueVector*/, common::ValueVector& resultVector,
        void* dataPtr);
};

struct StemBindData final : public FunctionBindData {
    sb_stemmer* sbStemmer = nullptr;
    std::string stemmer;

    StemBindData(common::logical_type_vec_t paramTypes, const std::string& stemmer)
        : FunctionBindData{std::move(paramTypes), common::LogicalType::STRING()}, stemmer{stemmer} {
        if (stemmer == "none") {
            return;
        }
        sbStemmer = sb_stemmer_new(stemmer.c_str(), "UTF_8");
        if (sbStemmer == nullptr) {
            throw common::RuntimeException(common::stringFormat(
                "Unrecognized stemmer '{}'. Supported stemmers are: ['{}'], or "
                "use 'none' for no stemming.",
                stemmer, getStemmerList()));
        }
    }

    ~StemBindData() override { sb_stemmer_delete(sbStemmer); }

    std::unique_ptr<FunctionBindData> copy() const override {
        return std::make_unique<StemBindData>(copyVector(paramTypes), stemmer);
    }
};

void StemStaticStemmer::operation(common::ku_string_t& word, common::ku_string_t& /*stemmer*/,
    common::ku_string_t& result, common::ValueVector& /*leftValueVector*/,
    common::ValueVector& /*rightValueVector*/, common::ValueVector& resultVector, void* dataPtr) {
    auto stemBindData = reinterpret_cast<StemBindData*>(dataPtr);
    KU_ASSERT(stemBindData->sbStemmer != nullptr);
    auto stemData = sb_stemmer_stem(stemBindData->sbStemmer,
        reinterpret_cast<const sb_symbol*>(word.getData()), word.len);
    common::StringVector::addString(&resultVector, result, reinterpret_cast<const char*>(stemData),
        sb_stemmer_length(stemBindData->sbStemmer));
}

void StemWithoutStemmer::operation(common::ku_string_t& word, common::ku_string_t& /*stemmer*/,
    common::ku_string_t& result, common::ValueVector& /*leftValueVector*/,
    common::ValueVector& /*rightValueVector*/, common::ValueVector& resultVector,
    void* /*dataPtr*/) {
    common::StringVector::addString(&resultVector, result,
        reinterpret_cast<const char*>(word.getData()), word.len);
}

static std::unique_ptr<FunctionBindData> stemBindFunc(const ScalarBindFuncInput& input) {
    if (input.arguments[1]->expressionType == ExpressionType::LITERAL) {
        auto value = evaluator::ExpressionEvaluatorUtils::evaluateConstantExpression(
            input.arguments[1], input.context);
        if (common::StringUtils::getLower(value.getValue<std::string>()) == "none") {
            input.definition->ptrCast<ScalarFunction>()->execFunc =
                ScalarFunction::BinaryExecWithBindData<ku_string_t, ku_string_t, ku_string_t,
                    StemWithoutStemmer>;
        } else {
            input.definition->ptrCast<ScalarFunction>()->execFunc =
                ScalarFunction::BinaryExecWithBindData<ku_string_t, ku_string_t, ku_string_t,
                    StemStaticStemmer>;
        }
        auto patternInStr = value.getValue<std::string>();
        return std::make_unique<StemBindData>(binder::ExpressionUtil::getDataTypes(input.arguments),
            patternInStr);
    } else {
        return FunctionBindData::getSimpleBindData(input.arguments, LogicalType::BOOL());
    }
}

function::function_set StemFunction::getFunctionSet() {
    function_set result;
    auto scalarFunction = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::STRING,
        ScalarFunction::BinaryStringExecFunction<ku_string_t, ku_string_t, ku_string_t, Stem>);
    scalarFunction->bindFunc = stemBindFunc;
    result.push_back(std::move(scalarFunction));
    return result;
}

void StemFunction::validateStemmer(const std::string& stemmer) {
    if (stemmer == "none") {
        return;
    }
    struct sb_stemmer* sbStemmer =
        sb_stemmer_new(reinterpret_cast<const char*>(stemmer.c_str()), "UTF_8");
    if (sbStemmer == nullptr) {
        throw common::BinderException(
            common::stringFormat("Unrecognized stemmer '{}'. Supported stemmers are: ['{}'], or "
                                 "use 'none' for no stemming.",
                stemmer, getStemmerList()));
    }
    sb_stemmer_delete(sbStemmer);
}

} // namespace fts_extension
} // namespace kuzu
