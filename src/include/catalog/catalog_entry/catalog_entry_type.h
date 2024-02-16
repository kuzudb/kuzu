#pragma once

#include <cstdint>

namespace kuzu {
namespace catalog {

enum class CatalogEntryType : uint8_t {
    NODE_TABLE_ENTRY = 0,
    REL_TABLE_ENTRY = 1,
    REL_GROUP_ENTRY = 2,
    RDF_GRAPH_ENTRY = 3,
};

} // namespace catalog
} // namespace kuzu
