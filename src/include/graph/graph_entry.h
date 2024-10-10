#pragma once

#include <string>
#include <unordered_map>

#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/assert.h"
#include "common/copy_constructors.h"
#include "common/types/types.h"

namespace kuzu {
namespace graph {

// Organize projected graph similar to CatalogEntry. When we want to share projected graph across
// statements, we need to migrate this class to catalog (or client context).
struct GraphEntry {
    std::vector<catalog::TableCatalogEntry*> nodeEntries;
    std::vector<catalog::TableCatalogEntry*> relEntries;

    binder::expression_vector relProperties;
    std::shared_ptr<binder::Expression> relPredicate;

    GraphEntry(std::vector<catalog::TableCatalogEntry*> nodeEntries,
        std::vector<catalog::TableCatalogEntry*> relEntries)
        : nodeEntries{std::move(nodeEntries)}, relEntries{std::move(relEntries)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(GraphEntry);

    bool hasRelEntry(common::table_id_t tableID) const { return getRelEntry(tableID) != nullptr; }

    const catalog::TableCatalogEntry* getRelEntry(common::table_id_t tableID) const {
        for (auto entry : relEntries) {
            if (entry->getTableID() == tableID) {
                return entry;
            }
        }
        return nullptr;
    }

private:
    GraphEntry(const GraphEntry& other)
        : nodeEntries{other.nodeEntries}, relEntries{other.relEntries},
          relProperties{other.relProperties}, relPredicate{other.relPredicate} {}
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
