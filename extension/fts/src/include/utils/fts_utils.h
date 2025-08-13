#pragma once

#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "function/fts_config.h"
#include "main/client_context.h"

namespace kuzu {
namespace storage {
class NodeTable;
}

namespace fts_extension {

struct FTSUtils {

    static void normalizeQuery(std::string& query, const regex::RE2& ignorePattern);

    static bool hasWildcardPattern(const std::string& term);

    static std::vector<std::string> stemTerms(std::vector<std::string> terms,
        const FTSConfig& config, storage::MemoryManager* mm, storage::NodeTable* stopwordsTable,
        transaction::Transaction* tx, bool isConjunctive, bool isQuery);

    static std::string getDefaultStopWordsTableName() {
        return common::stringFormat("default_english_stopwords");
    }

    static std::string getInternalTablePrefix(common::table_id_t tableID,
        const std::string& indexName) {
        return common::stringFormat("{}_{}", tableID, indexName);
    }

    static std::string getNonDefaultStopWordsTableName(common::table_id_t tableID,
        const std::string& indexName) {
        return common::stringFormat("{}_stopwords", getInternalTablePrefix(tableID, indexName));
    }

    static std::string getDocsTableName(common::table_id_t tableID, const std::string& indexName) {
        return common::stringFormat("{}_docs", getInternalTablePrefix(tableID, indexName));
    }

    static std::string getAppearsInfoTableName(common::table_id_t tableID,
        const std::string& indexName) {
        return common::stringFormat("{}_appears_info", getInternalTablePrefix(tableID, indexName));
    }

    static std::string getTermsTableName(common::table_id_t tableID, const std::string& indexName) {
        return common::stringFormat("{}_terms", getInternalTablePrefix(tableID, indexName));
    }

    static std::string getAppearsInTableName(common::table_id_t tableID,
        const std::string& indexName) {
        return common::stringFormat("{}_appears_in", getInternalTablePrefix(tableID, indexName));
    }

    static std::string getTokenizeMacroName(common::table_id_t tableID,
        const std::string& indexName) {
        return common::stringFormat("{}_tokenize", getInternalTablePrefix(tableID, indexName));
    }
};

} // namespace fts_extension
} // namespace kuzu
