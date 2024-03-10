#include "catalog/catalog_entry/catalog_entry_type.h"

#include "common/assert.h"

namespace kuzu {
namespace catalog {

// LCOV_EXCL_START
std::string CatalogEntryTypeUtils::toString(kuzu::catalog::CatalogEntryType catalogEntryType) {
    switch (catalogEntryType) {
    case catalog::CatalogEntryType::NODE_TABLE_ENTRY:
        return "NODE_TABLE";
    case catalog::CatalogEntryType::REL_TABLE_ENTRY:
        return "REL_TABLE";
    case catalog::CatalogEntryType::REL_GROUP_ENTRY:
        return "REL_GROUP";
    case catalog::CatalogEntryType::RDF_GRAPH_ENTRY:
        return "RDF_GRAPH";
    case catalog::CatalogEntryType::SCALAR_MACRO_ENTRY:
        return "SCALAR_MACRO";
    case catalog::CatalogEntryType::AGGREGATE_FUNCTION_ENTRY:
        return "AGGREGATE_FUNCTION";
    case catalog::CatalogEntryType::SCALAR_FUNCTION_ENTRY:
        return "SCALAR_FUNCTION";
    case catalog::CatalogEntryType::TABLE_FUNCTION_ENTRY:
        return "TABLE_FUNCTION";
    default:
        KU_UNREACHABLE;
    }
}
// LCOV_EXCL_STOP

} // namespace catalog
} // namespace kuzu
