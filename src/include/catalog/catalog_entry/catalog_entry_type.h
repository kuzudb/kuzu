#pragma once

#include <cstdint>
#include <string>

namespace kuzu {
namespace catalog {

enum class CatalogEntryType : uint8_t {
    // Table entries
    NODE_TABLE_ENTRY = 0,
    REL_TABLE_ENTRY = 1,
    REL_GROUP_ENTRY = 2,
    RDF_GRAPH_ENTRY = 3,
    FOREIGN_TABLE_ENTRY = 4,
    // Macro entries
    SCALAR_MACRO_ENTRY = 10,
    // Function entries
    AGGREGATE_FUNCTION_ENTRY = 20,
    SCALAR_FUNCTION_ENTRY = 21,
    REWRITE_FUNCTION_ENTRY = 22,
    TABLE_FUNCTION_ENTRY = 23,
    GDS_FUNCTION_ENTRY = 24,
    COPY_FUNCTION_ENTRY = 25,
    // Sequence entries
    SEQUENCE_ENTRY = 40,
    // UDT entries
    TYPE_ENTRY = 41,
    // Dummy entry
    DUMMY_ENTRY = 100,
};

struct CatalogEntryTypeUtils {
    static std::string toString(CatalogEntryType type);
};

} // namespace catalog
} // namespace kuzu
