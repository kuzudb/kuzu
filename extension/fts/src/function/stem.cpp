#include "function/stem.h"

#include "common/exception/binder.h"
#include "common/exception/runtime.h"
#include "common/types/ku_string.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
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

function::function_set StemFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::STRING,
        ScalarFunction::BinaryStringExecFunction<ku_string_t, ku_string_t, ku_string_t, Stem>));
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
