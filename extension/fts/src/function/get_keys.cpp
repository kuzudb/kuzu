#include "function/get_keys.h"

#include "common/exception/binder.h"
#include "common/string_utils.h"
#include "function/scalar_function.h"
#include "function/stem.h"
#include "function/string/functions/lower_function.h"
#include "libstemmer.h"
#include "re2.h"

namespace kuzu {
namespace fts_extension {

using namespace function;
using namespace common;

struct GetKeys {
    static void operation(common::ku_string_t& word, common::ku_string_t& stemmer,
        common::list_entry_t& result, common::ValueVector& resultVector);
};

void GetKeys::operation(common::ku_string_t& query, common::ku_string_t& stemmer,
    common::list_entry_t& result, common::ValueVector& resultVector) {
    std::string regexPattern = "[0-9!@#$%^&*()_+={}\\[\\]:;<>,.?~\\/\\|'\"`-]+";
    std::string replacePattern = " ";
    std::string resultStr = query.getAsString();
    RE2::GlobalReplace(&resultStr, regexPattern, replacePattern);
    StringUtils::toLower(resultStr);
    auto words = StringUtils::split(resultStr, " ");
    StemFunction::validateStemmer(stemmer.getAsString());
    auto sbStemmer = sb_stemmer_new(reinterpret_cast<const char*>(stemmer.getData()), "UTF_8");
    result = ListVector::addList(&resultVector, words.size());
    for (auto i = 0u; i < result.size; i++) {
        auto& word = words[i];
        auto stemData = sb_stemmer_stem(sbStemmer, reinterpret_cast<const sb_symbol*>(word.c_str()),
            word.length());
        ListVector::getDataVector(&resultVector)
            ->setValue(result.offset + i,
                std::string_view{reinterpret_cast<const char*>(stemData)});
    }
    sb_stemmer_delete(sbStemmer);
}

static std::unique_ptr<FunctionBindData> bindFunc(ScalarBindFuncInput input) {
    return FunctionBindData::getSimpleBindData(input.arguments,
        LogicalType::LIST(LogicalType::STRING()));
}

function::function_set GetKeysFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::LIST,
        ScalarFunction::BinaryStringExecFunction<ku_string_t, ku_string_t, list_entry_t, GetKeys>,
        bindFunc));
    return result;
}

} // namespace fts_extension
} // namespace kuzu
