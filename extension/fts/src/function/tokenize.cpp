#include "function/tokenize.h"

#include "common/string_utils.h"
#include "common/exception/runtime.h"
#include "common/types/ku_string.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "function/scalar_function.h"
#include "re2.h"

#if defined(KUZU_ENABLE_JIEBA)
#include "cppjieba/Jieba.hpp"
#endif

namespace kuzu {
namespace fts_extension {

using namespace function;
using namespace common;

/**
 * TOKENIZE scalar function for FTS text tokenization
 * Supports multiple tokenization strategies including Chinese text segmentation
 */
struct Tokenize {
    static void operation(common::ku_string_t& text, common::ku_string_t& tokenizerName,
        common::ku_string_t& extraParam, list_entry_t& result, common::ValueVector& resultVector) {
        (void)extraParam; // Reserved for future tokenizer-specific parameters
        std::string s = text.getAsString();

        auto kindStr = StringUtils::getLower(tokenizerName.getAsString());
        std::vector<std::string> tokens;
        if (kindStr == "jieba") {
#if defined(KUZU_ENABLE_JIEBA)
            // Use Jieba Chinese word segmentation
            // Static initialization ensures dictionary loading happens only once per session
            static std::unique_ptr<cppjieba::Jieba> jieba;
            static std::once_flag initFlag;
            std::call_once(initFlag, [&]() {
                std::string dictDir = extraParam.getAsString();
                std::string dict = dictDir + "/jieba.dict.utf8";
                std::string hmm = dictDir + "/hmm_model.utf8";
                std::string user = dictDir + "/user.dict.utf8";  // Contains custom AI/ML terms
                std::string idf = dictDir + "/idf.utf8";
                std::string stop = dictDir + "/stop_words.utf8";
                jieba = std::make_unique<cppjieba::Jieba>(dict, hmm, user, idf, stop);
            });
            // CutForSearch mode provides both individual terms and compound terms for better recall
            jieba->CutForSearch(s, tokens);
#else
            throw common::RuntimeException("Jieba tokenizer not enabled at build time. "
                                         "Rebuild with -DKUZU_ENABLE_JIEBA=ON");
#endif
        } else {
            // Default whitespace tokenization: split on whitespace
            // Handles 'whitespace', 'simple' (legacy), and any other values
            tokens = StringUtils::split(s, " ", true /* ignoreEmptyStringParts */);
        }

        result = ListVector::addList(&resultVector, tokens.size());
        for (auto i = 0u; i < tokens.size(); i++) {
            ListVector::getDataVector(&resultVector)->setValue(result.offset + i, tokens[i]);
        }
    }
};

static std::unique_ptr<FunctionBindData> bindFunc(const ScalarBindFuncInput& input) {
    return FunctionBindData::getSimpleBindData(input.arguments,
        LogicalType::LIST(LogicalType::STRING()));
}

function::function_set TokenizeFunction::getFunctionSet() {
    function_set result;
    auto fn = std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING,
            LogicalTypeID::STRING},
        LogicalTypeID::LIST,
        ScalarFunction::TernaryStringExecFunction<ku_string_t, ku_string_t, ku_string_t,
            list_entry_t, Tokenize>);
    fn->bindFunc = bindFunc;
    result.push_back(std::move(fn));
    return result;
}

} // namespace fts_extension
} // namespace kuzu


