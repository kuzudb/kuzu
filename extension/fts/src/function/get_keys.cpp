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

std::vector<std::string> getTerms(std::string& query, const std::string& stemmer) {
    std::string regexPattern = "[0-9!@#$%^&*()_+={}\\[\\]:;<>,.?~\\/\\|'\"`-]+";
    std::string replacePattern = " ";
    RE2::GlobalReplace(&query, regexPattern, replacePattern);
    StringUtils::toLower(query);
    auto terms = StringUtils::split(query, " ");
    StemFunction::validateStemmer(stemmer);
    auto sbStemmer = sb_stemmer_new(reinterpret_cast<const char*>(stemmer.c_str()), "UTF_8");
    std::vector<std::string> result;
    for (auto& term : terms) {
        auto stemData = sb_stemmer_stem(sbStemmer, reinterpret_cast<const sb_symbol*>(term.c_str()),
            term.length());
        result.push_back(reinterpret_cast<const char*>(stemData));
    }
    sb_stemmer_delete(sbStemmer);
    return result;
}

} // namespace fts_extension
} // namespace kuzu
