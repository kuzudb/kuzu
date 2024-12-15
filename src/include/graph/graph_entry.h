#pragma once

#include <string>
#include <unordered_map>

#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/assert.h"
#include "common/copy_constructors.h"
#include "common/types/types.h"

namespace kuzu {
namespace planner {
class Schema;
}
namespace graph {

// Organize projected graph similar to CatalogEntry. When we want to share projected graph across
// statements, we need to migrate this class to catalog (or client context).
struct GraphEntry {
    std::vector<catalog::TableCatalogEntry*> nodeEntries;
    std::vector<catalog::TableCatalogEntry*> relEntries;

    GraphEntry(std::vector<catalog::TableCatalogEntry*> nodeEntries,
        std::vector<catalog::TableCatalogEntry*> relEntries)
        : nodeEntries{std::move(nodeEntries)}, relEntries{std::move(relEntries)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(GraphEntry);

    bool hasRelEntry(common::table_id_t tableID) const { return getRelEntry(tableID) != nullptr; }
    const catalog::TableCatalogEntry* getRelEntry(common::table_id_t tableID) const;

    bool hasRelPredicate() const { return relPredicate != nullptr; }
    std::shared_ptr<binder::Expression> getRelPredicate() const { return relPredicate; }
    void setRelPredicate(std::shared_ptr<binder::Expression> predicate);
    binder::expression_vector getRelProperties() const { return relProperties; }

    planner::Schema getRelPropertiesSchema() const;

private:
    GraphEntry(const GraphEntry& other)
        : nodeEntries{other.nodeEntries}, relEntries{other.relEntries},
          relProperties{other.relProperties}, relPredicate{other.relPredicate} {}

private:
    binder::expression_vector relProperties;
    std::shared_ptr<binder::Expression> relPredicate;
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
    void dropGraph(const std::string& name) { nameToEntry.erase(name); }

private:
    std::unordered_map<std::string, GraphEntry> nameToEntry;
};

} // namespace graph
} // namespace kuzu
