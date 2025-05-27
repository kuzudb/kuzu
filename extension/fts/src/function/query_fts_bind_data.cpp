#include "function/query_fts_bind_data.h"

#include "binder/expression/expression_util.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "catalog/fts_index_catalog_entry.h"
#include "common/exception/binder.h"
#include "common/string_utils.h"
#include "function/stem.h"
#include "libstemmer.h"
#include "re2.h"
#include "storage/storage_manager.h"
#include "storage/table/node_table.h"

namespace kuzu {
namespace fts_extension {

using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::storage;

QueryFTSOptionalParams::QueryFTSOptionalParams(const binder::expression_vector& optionalParams) {
    for (auto& optionalParam : optionalParams) {
        auto paramName = StringUtils::getLower(optionalParam->getAlias());
        if (paramName == K::NAME) {
            k = optionalParam;
        } else if (paramName == B::NAME) {
            b = optionalParam;
        } else if (paramName == Conjunctive::NAME) {
            conjunctive = optionalParam;
        } else {
            throw common::BinderException{"Unknown optional parameter: " + paramName};
        }
    }
}

QueryFTSConfig QueryFTSOptionalParams::getConfig() const {
    QueryFTSConfig config;
    if (k != nullptr) {
        config.k = ExpressionUtil::evaluateLiteral<double>(*k, LogicalType::DOUBLE(), K::validate);
    }
    if (b != nullptr) {
        config.b = ExpressionUtil::evaluateLiteral<double>(*b, LogicalType::DOUBLE(), B::validate);
    }
    if (conjunctive != nullptr) {
        config.isConjunctive =
            ExpressionUtil::evaluateLiteral<bool>(*conjunctive, LogicalType::BOOL());
    }
    return config;
}

static void normalizeQuery(std::string& query) {
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

static std::vector<std::string> stemTerms(std::vector<std::string> terms, const FTSConfig& config,
    main::ClientContext& context, bool isConjunctive) {
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

std::vector<std::string> QueryFTSBindData::getTerms(main::ClientContext& context) const {
    auto queryInStr = ExpressionUtil::evaluateLiteral<std::string>(*query, LogicalType::STRING());
    normalizeQuery(queryInStr);
    auto terms = StringUtils::split(queryInStr, " ");
    return stemTerms(terms, entry.getAuxInfo().cast<FTSIndexAuxInfo>().config, context,
        getConfig().isConjunctive);
}

} // namespace fts_extension
} // namespace kuzu
