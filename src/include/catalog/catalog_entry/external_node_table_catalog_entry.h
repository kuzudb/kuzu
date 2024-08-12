#pragma once

#include "external_table_catalog_entry.h"

namespace kuzu {
namespace catalog {

class ExternalNodeTableCatalogEntry : public ExternalTableCatalogEntry {
    static constexpr CatalogEntryType entryType_ = CatalogEntryType::EXTERNAL_NODE_TABLE_ENTRY;
public:
    ExternalNodeTableCatalogEntry() = default;
    ExternalNodeTableCatalogEntry(CatalogSet* set, std::string name, std::string externalDBName,
        std::string externalTableName, std::unique_ptr<TableCatalogEntry> physicalEntry,
        std::string primaryKeyName)
        : ExternalTableCatalogEntry{set, entryType_, name, externalDBName, externalTableName,
              std::move(physicalEntry)}, primaryKeyName{std::move(primaryKeyName)} {}

    common::TableType getTableType() const override {
        return common::TableType::EXTERNAL_NODE;
    }

    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<ExternalNodeTableCatalogEntry> deserialize(common::Deserializer& deserializer);

    std::unique_ptr<TableCatalogEntry> copy() const override;

private:
    std::unique_ptr<binder::BoundExtraCreateCatalogEntryInfo> getBoundExtraCreateInfo(
        transaction::Transaction* transaction) const override;

private:
    std::string primaryKeyName;
};

}
}
