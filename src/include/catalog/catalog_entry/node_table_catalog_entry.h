#pragma once

#include "index_catalog_entry.h"
#include "table_catalog_entry.h"

namespace kuzu {
namespace transaction {
class Transaction;
} // namespace transaction

namespace catalog {

class CatalogSet;
class KUZU_API NodeTableCatalogEntry final : public TableCatalogEntry {
    static constexpr CatalogEntryType entryType_ = CatalogEntryType::NODE_TABLE_ENTRY;

public:
    NodeTableCatalogEntry() = default;
    NodeTableCatalogEntry(CatalogSet* set, std::string name, std::string primaryKeyName)
        : TableCatalogEntry{set, entryType_, std::move(name)},
          primaryKeyName{std::move(primaryKeyName)} {}

    bool isParent(common::table_id_t /*tableID*/) override { return false; }
    common::TableType getTableType() const override { return common::TableType::NODE; }

    std::string getPrimaryKeyName() const { return primaryKeyName; }
    common::idx_t getPrimaryKeyIdx() const { return propertyCollection.getIdx(primaryKeyName); }
    const binder::PropertyDefinition& getPrimaryKeyDefinition() const {
        return getProperty(primaryKeyName);
    }

    void addIndex(std::string name) {
        indexes.emplace(name, std::make_unique<IndexCatalogEntry>(name, getTableID()));
    }

    bool containsIndex(std::string name) { return indexes.contains(name); }

    void dropIndex(std::string name) { indexes.erase(name); }

    std::unique_ptr<TableCatalogEntry> alter(
        const binder::BoundAlterInfo& alterInfo) const override;

    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<NodeTableCatalogEntry> deserialize(common::Deserializer& deserializer);

    std::unique_ptr<TableCatalogEntry> copy() const override;
    std::string toCypher(main::ClientContext* /*clientContext*/) const override;

private:
    std::unique_ptr<binder::BoundExtraCreateCatalogEntryInfo> getBoundExtraCreateInfo(
        transaction::Transaction* transaction) const override;

private:
    std::string primaryKeyName;
    common::case_insensitive_map_t<std::unique_ptr<IndexCatalogEntry>> indexes;
};

} // namespace catalog
} // namespace kuzu
