#pragma once

#include <string>

namespace kuzu {
namespace catalog {
class TableCatalogEntry;
class NodeTableCatalogEntry;
} // namespace catalog

namespace main {
class ClientContext;
}

namespace fts_extension {

struct FTSIndexUtils {
    enum class IndexOperation { CREATE, QUERY, DROP };

    static void validateIndexExistence(const main::ClientContext& context,
        const catalog::TableCatalogEntry* tableEntry, const std::string& indexName,
        IndexOperation indexOperation);

    static catalog::NodeTableCatalogEntry* bindNodeTable(const main::ClientContext& context,
        const std::string& tableName, const std::string& indexName, IndexOperation indexOperation);

    static void validateAutoTransaction(const main::ClientContext& context,
        const std::string& funcName);
};

} // namespace fts_extension
} // namespace kuzu
