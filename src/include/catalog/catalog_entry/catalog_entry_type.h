#pragma once

#include <cstdint>

namespace kuzu {
namespace catalog {

enum class CatalogEntryType : uint8_t {
    NODE_TABLE_ENTRY = 0,
    REL_TABLE_ENTRY = 1,
    REL_GROUP_ENTRY = 2,
    RDF_GRAPH_ENTRY = 3,
    SCALAR_MACRO_ENTRY = 4,
    AGGREGATE_FUNCTION_ENTRY = 5,
    SCALAR_FUNCTION_ENTRY = 6,
    REWRITE_FUNCTION_ENTRY = 7,
    TABLE_FUNCTION_ENTRY = 8,
    FOREIGN_TABLE_ENTRY = 9,
};

} // namespace catalog
} // namespace kuzu
