#pragma once

#include <string>
#include <unordered_map>

#include "common/assert.h"
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

class GraphEntrySet {
public:
    bool hasGraph(const std::string& name) const { return nameToEntry.contains(name); }
    GraphEntry getEntry(const std::string& name) const {
        KU_ASSERT(hasGraph(name));
        return nameToEntry.at(name).copy();
    }
    void addGraph(const std::string& name, const GraphEntry& entry) {
        nameToEntry.insert({name, entry.copy()});
    }

private:
    std::unordered_map<std::string, GraphEntry> nameToEntry;
};

} // namespace graph
} // namespace kuzu
