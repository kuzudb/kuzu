#pragma once

#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "main/client_context.h"

namespace kuzu {
namespace fts_extension {

struct FTSUtils {

    static std::string getStopWordsTableName() {
        return common::stringFormat("default_english_stopwords");
    }

    static std::string getInternalTablePrefix(common::table_id_t tableID,
        const std::string& indexName) {
        return common::stringFormat("{}_{}", tableID, indexName);
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
};

} // namespace fts_extension
} // namespace kuzu
