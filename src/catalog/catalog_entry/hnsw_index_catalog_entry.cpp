#include "catalog/catalog_entry/hnsw_index_catalog_entry.h"

namespace kuzu {
namespace catalog {

std::unique_ptr<IndexCatalogEntry> HNSWIndexCatalogEntry::copy() const {
    return std::make_unique<HNSWIndexCatalogEntry>(tableID, indexName, columnName, upperRelTableID,
        lowerRelTableID, upperEntryPoint, lowerEntryPoint);
}

} // namespace catalog
} // namespace kuzu
