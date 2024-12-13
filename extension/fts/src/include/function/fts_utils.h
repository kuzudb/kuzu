#pragma once

#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "common/types/value/value.h"
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
};

} // namespace fts_extension
} // namespace kuzu
