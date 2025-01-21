#pragma once

#include "table_catalog_entry.h"

namespace kuzu {
namespace transaction {
class Transaction;
} // namespace transaction

namespace catalog {

class Catalog;
class KUZU_API NodeTableCatalogEntry final : public TableCatalogEntry {
    static constexpr CatalogEntryType entryType_ = CatalogEntryType::NODE_TABLE_ENTRY;

public:
    NodeTableCatalogEntry() = default;
    NodeTableCatalogEntry(std::string name, std::string primaryKeyName)
        : TableCatalogEntry{entryType_, std::move(name)},
          primaryKeyName{std::move(primaryKeyName)} {}

    bool isParent(common::table_id_t /*tableID*/) override { return false; }
    common::TableType getTableType() const override { return common::TableType::NODE; }

    std::string getPrimaryKeyName() const { return primaryKeyName; }
    common::idx_t getPrimaryKeyIdx() const { return propertyCollection.getIdx(primaryKeyName); }
    const binder::PropertyDefinition& getPrimaryKeyDefinition() const {
        return getProperty(primaryKeyName);
    }

    common::table_id_set_t getFwdRelTableIDs(Catalog* catalog,
        transaction::Transaction* transaction) const;
    common::table_id_set_t getBwdRelTableIDs(Catalog* catalog,
        transaction::Transaction* transaction) const;

    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<NodeTableCatalogEntry> deserialize(common::Deserializer& deserializer);

    std::unique_ptr<TableCatalogEntry> copy() const override;
    std::string toCypher(main::ClientContext* /*clientContext*/) const override;

private:
    std::unique_ptr<binder::BoundExtraCreateCatalogEntryInfo> getBoundExtraCreateInfo(
        transaction::Transaction* transaction) const override;

private:
    std::string primaryKeyName;
};

} // namespace catalog
} // namespace kuzu
