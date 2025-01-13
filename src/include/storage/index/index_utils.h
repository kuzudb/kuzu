#pragma once

#include <string>

#include "common/api.h"

namespace kuzu {
namespace catalog {
class NodeTableCatalogEntry;
} // namespace catalog

namespace main {
class ClientContext;
} // namespace main

namespace storage {

enum class IndexOperation { CREATE, QUERY, DROP };

struct IndexUtils {
    static KUZU_API catalog::NodeTableCatalogEntry* bindTable(const main::ClientContext& context,
        const std::string& tableName, const std::string& indexName, IndexOperation indexOperation);

    static KUZU_API void validateAutoTransaction(const main::ClientContext& context,
        const std::string& funcName);
};

} // namespace storage
} // namespace kuzu
