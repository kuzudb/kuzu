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

struct KUZU_API GraphEntryTableInfo {
    catalog::TableCatalogEntry* entry;

    std::shared_ptr<binder::Expression> nodeOrRel;
    std::shared_ptr<binder::Expression> predicate;

    explicit GraphEntryTableInfo(catalog::TableCatalogEntry* entry)
        : entry{entry}, predicate{nullptr} {}
    GraphEntryTableInfo(catalog::TableCatalogEntry* entry,
        std::shared_ptr<binder::Expression> nodeOrRel,
        std::shared_ptr<binder::Expression> predicate)
        : entry{entry}, nodeOrRel{std::move(nodeOrRel)}, predicate{std::move(predicate)} {}

    void setPredicate(std::shared_ptr<binder::Expression> predicate_);
};

// Organize projected graph similar to CatalogEntry. When we want to share projected graph across
// statements, we need to migrate this class to catalog (or client context).
struct KUZU_API GraphEntry {
    std::vector<GraphEntryTableInfo> nodeInfos;
    std::vector<GraphEntryTableInfo> relInfos;

    GraphEntry() = default;
    GraphEntry(std::vector<catalog::TableCatalogEntry*> nodeEntries,
        std::vector<catalog::TableCatalogEntry*> relEntries);
    EXPLICIT_COPY_DEFAULT_MOVE(GraphEntry);

    std::vector<common::table_id_t> getNodeTableIDs() const;
    std::vector<common::table_id_t> getRelTableIDs() const;
    std::vector<catalog::TableCatalogEntry*> getNodeEntries() const;
    std::vector<catalog::TableCatalogEntry*> getRelEntries() const;

    const GraphEntryTableInfo& getRelInfo(common::table_id_t tableID) const;

    void setRelPredicate(std::shared_ptr<binder::Expression> predicate);

private:
    GraphEntry(const GraphEntry& other) : nodeInfos{other.nodeInfos}, relInfos{other.relInfos} {}
};

class GraphEntrySet {
public:
    bool hasGraph(const std::string& name) const { return nameToEntry.contains(name); }
    const GraphEntry& getEntry(const std::string& name) const {
        KU_ASSERT(hasGraph(name));
        return nameToEntry.at(name);
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
