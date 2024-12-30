#pragma once

#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "main/client_context.h"

namespace kuzu {
namespace fts_extension {

struct FTSUtils {

    enum class IndexOperation : uint8_t {
        CREATE = 0,
        QUERY = 1,
        DROP = 2,
    };

    static catalog::NodeTableCatalogEntry& bindTable(const std::string& tableName,
        main::ClientContext* context, std::string indexName, IndexOperation indexOperation);

    static void validateIndexExistence(const main::ClientContext& context,
        common::table_id_t tableID, std::string indexName);

    static void validateAutoTrx(const main::ClientContext& context, const std::string& funcName);

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
