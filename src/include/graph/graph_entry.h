#pragma once

#include "common/copy_constructors.h"
#include "common/types/internal_id_t.h"

namespace kuzu {
namespace graph {

// Organize projected graph similar to CatalogEntry. When we want to share projected graph across
// statements, we need to migrate this class to catalog (or client context).
struct GraphEntry {
    std::vector<common::table_id_t> nodeTableIDs;
    std::vector<common::table_id_t> relTableIDs;

    GraphEntry(std::vector<common::table_id_t> nodeTableIDs,
        std::vector<common::table_id_t> relTableIDs)
        : nodeTableIDs{std::move(nodeTableIDs)}, relTableIDs{std::move(relTableIDs)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(GraphEntry);

private:
    GraphEntry(const GraphEntry& other)
        : nodeTableIDs(other.nodeTableIDs), relTableIDs(other.relTableIDs) {}
};

} // namespace graph
} // namespace kuzu
