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

void FTSUtils::normalizeQuery(std::string& query) {
    std::string regexPattern = "[0-9!@#$%^&*()_+={}\\[\\]:;<>,.?~\\/\\|'\"`-]+";
    std::string replacePattern = " ";
    RE2::GlobalReplace(&query, regexPattern, replacePattern);
    StringUtils::toLower(query);
}

struct StopWordsChecker {
    ValueVector termsVector;
    common::offset_t offset = common::INVALID_OFFSET;
    storage::NodeTable* stopWordsTable;
    transaction::Transaction* tx;

    explicit StopWordsChecker(main::ClientContext& context, const std::string& stopWordsTableName);
    bool isStopWord(const std::string& term);
};

StopWordsChecker::StopWordsChecker(main::ClientContext& context,
    const std::string& stopWordsTableName)
    : termsVector{LogicalType::STRING(), context.getMemoryManager()}, tx{context.getTransaction()} {
    termsVector.state = common::DataChunkState::getSingleValueDataChunkState();
    auto tableID = context.getCatalog()->getTableCatalogEntry(tx, stopWordsTableName)->getTableID();
    stopWordsTable = context.getStorageManager()->getTable(tableID)->ptrCast<storage::NodeTable>();
}

bool StopWordsChecker::isStopWord(const std::string& term) {
    termsVector.setValue(0, term);
    return stopWordsTable->lookupPK(tx, &termsVector, 0 /* vectorPos */, offset);
}

std::vector<std::string> FTSUtils::stemTerms(std::vector<std::string> terms,
    const FTSConfig& config, main::ClientContext& context, bool isConjunctive) {
    if (config.stemmer == "none") {
        return terms;
    }
    StemFunction::validateStemmer(config.stemmer);
    auto sbStemmer = sb_stemmer_new(reinterpret_cast<const char*>(config.stemmer.c_str()), "UTF_8");
    std::vector<std::string> result;
    StopWordsChecker checker{context, config.stopWordsTableName};
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
