#include "function/query_fts_bind_data.h"

#include "binder/expression/expression_util.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "catalog/fts_index_catalog_entry.h"
#include "common/string_utils.h"
#include "function/fts_utils.h"
#include "function/stem.h"
#include "libstemmer.h"
#include "re2.h"
#include "storage/storage_manager.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace fts_extension {

using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::storage;

static std::string evaluateQuery(const Expression& query) {
    if (!ExpressionUtil::canEvaluateAsLiteral(query)) {
        std::string errMsg;
        switch (query.expressionType) {
        case ExpressionType::PARAMETER: {
            errMsg = "The query is a parameter expression. Please assign it a value.";
        } break;
        default: {
            errMsg = "The query must be a parameter/literal expression.";
        } break;
        }
        throw RuntimeException{errMsg};
    }
    auto value = binder::ExpressionUtil::evaluateAsLiteralValue(query);
    if (value.getDataType() != common::LogicalType::STRING()) {
        throw RuntimeException{"The query must be a string literal."};
    }
    return value.getValue<std::string>();
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

    explicit StopWordsChecker(main::ClientContext& context);
    bool isStopWord(const std::string& term);
};

StopWordsChecker::StopWordsChecker(main::ClientContext& context)
    : termsVector{LogicalType::STRING(), context.getMemoryManager()}, tx{context.getTransaction()} {
    termsVector.state = common::DataChunkState::getSingleValueDataChunkState();
    auto tableID = context.getCatalog()
                       ->getTableCatalogEntry(tx, FTSUtils::getStopWordsTableName())
                       ->getTableID();
    stopWordsTable = context.getStorageManager()->getTable(tableID)->ptrCast<storage::NodeTable>();
}

bool StopWordsChecker::isStopWord(const std::string& term) {
    termsVector.setValue(0, term);
    return stopWordsTable->lookupPK(tx, &termsVector, 0 /* vectorPos */, offset);
}

static std::vector<std::string> stemTerms(std::vector<std::string> terms,
    const std::string& stemmer, main::ClientContext& context, bool isConjunctive) {
    if (stemmer == "none") {
        return terms;
    }
    StemFunction::validateStemmer(stemmer);
    auto sbStemmer = sb_stemmer_new(reinterpret_cast<const char*>(stemmer.c_str()), "UTF_8");
    std::vector<std::string> result;
    StopWordsChecker checker{context};
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
    auto queryInStr = evaluateQuery(*query);
    normalizeQuery(queryInStr);
    auto terms = StringUtils::split(queryInStr, " ");
    return stemTerms(terms, entry.getAuxInfo().cast<FTSIndexAuxInfo>().config.stemmer, context,
        config.isConjunctive);
}

} // namespace fts_extension
} // namespace kuzu
