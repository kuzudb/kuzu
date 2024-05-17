#include "catalog/catalog_entry/catalog_entry_type.h"

#include "common/assert.h"

namespace kuzu {
namespace catalog {

std::string CatalogEntryTypeUtils::toString(CatalogEntryType type) {
    switch (type) {
    case CatalogEntryType::NODE_TABLE_ENTRY:
        return "NODE_TABLE_ENTRY";
    case CatalogEntryType::REL_TABLE_ENTRY:
        return "REL_TABLE_ENTRY";
    case CatalogEntryType::REL_GROUP_ENTRY:
        return "REL_GROUP_ENTRY";
    case CatalogEntryType::RDF_GRAPH_ENTRY:
        return "RDF_GRAPH_ENTRY";
    case CatalogEntryType::SCALAR_MACRO_ENTRY:
        return "SCALAR_MACRO_ENTRY";
    case CatalogEntryType::AGGREGATE_FUNCTION_ENTRY:
        return "AGGREGATE_FUNCTION_ENTRY";
    case CatalogEntryType::SCALAR_FUNCTION_ENTRY:
        return "SCALAR_FUNCTION_ENTRY";
    case CatalogEntryType::REWRITE_FUNCTION_ENTRY:
        return "REWRITE_FUNCTION_ENTRY";
    case CatalogEntryType::TABLE_FUNCTION_ENTRY:
        return "TABLE_FUNCTION_ENTRY";
    case CatalogEntryType::FOREIGN_TABLE_ENTRY:
        return "FOREIGN_TABLE_ENTRY";
    case CatalogEntryType::DUMMY_ENTRY:
        return "DUMMY_ENTRY";
    case CatalogEntryType::SEQUENCE_ENTRY:
        return "SEQUENCE_ENTRY";
    default:
        KU_UNREACHABLE;
    }
}

} // namespace catalog
} // namespace kuzu
