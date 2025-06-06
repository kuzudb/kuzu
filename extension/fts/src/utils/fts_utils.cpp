#include "utils/fts_utils.h"

#include "common/string_utils.h"
#include "function/stem.h"
#include "libstemmer.h"
#include "re2.h"
#include "storage/storage_manager.h"
#include "storage/table/node_table.h"

namespace kuzu {
namespace fts_extension {

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::transaction;
using namespace kuzu::catalog;

void FTSUtils::normalizeQuery(std::string& query) {
    std::string regexPattern = "[0-9!@#$%^&*()_+={}\\[\\]:;<>,.?~\\/\\|'\"`-]+";
    std::string replacePattern = " ";
    RE2::GlobalReplace(&query, regexPattern, replacePattern);
    StringUtils::toLower(query);
}

struct StopWordsChecker {
    ValueVector termsVector;
    storage::NodeTable* stopWordsTable;
    transaction::Transaction* tx;
    common::offset_t offset = common::INVALID_OFFSET;

    explicit StopWordsChecker(MemoryManager* mm, NodeTable* stopWordsTable, Transaction* tx);
    bool isStopWord(const std::string& term);
};

StopWordsChecker::StopWordsChecker(MemoryManager* mm, NodeTable* stopwordsTable, Transaction* tx)
    : termsVector{LogicalType::STRING(), mm}, stopWordsTable{stopwordsTable}, tx{tx} {
    termsVector.state = common::DataChunkState::getSingleValueDataChunkState();
}

bool StopWordsChecker::isStopWord(const std::string& term) {
    termsVector.setValue(0, term);
    return stopWordsTable->lookupPK(tx, &termsVector, 0 /* vectorPos */, offset);
}

std::vector<std::string> FTSUtils::stemTerms(std::vector<std::string> terms,
    const FTSConfig& config, MemoryManager* mm, NodeTable* stopwordsTable, Transaction* tx,
    bool isConjunctive) {
    if (config.stemmer == "none") {
        return terms;
    }
    StemFunction::validateStemmer(config.stemmer);
    auto sbStemmer = sb_stemmer_new(reinterpret_cast<const char*>(config.stemmer.c_str()), "UTF_8");
    std::vector<std::string> result;
    StopWordsChecker checker{mm, stopwordsTable, tx};
    for (auto& term : terms) {
        auto stemData = sb_stemmer_stem(sbStemmer, reinterpret_cast<const sb_symbol*>(term.c_str()),
            term.length());
        std::string stemWord = std::string(reinterpret_cast<const char*>(stemData));
        if (isConjunctive && checker.isStopWord(stemWord)) {
            continue;
        }
        result.push_back(stemWord);
    }
    sb_stemmer_delete(sbStemmer);
    return result;
}

} // namespace fts_extension
} // namespace kuzu
