#pragma once

#include "table_catalog_entry.h"

namespace kuzu {
namespace catalog {

class RelGroupCatalogEntry final : public TableCatalogEntry {
public:
    //===--------------------------------------------------------------------===//
    // constructors
    //===--------------------------------------------------------------------===//
    RelGroupCatalogEntry() = default;
    RelGroupCatalogEntry(CatalogSet* set, std::string tableName,
        std::vector<common::table_id_t> relTableIDs);

    //===--------------------------------------------------------------------===//
    // getter & setter
    //===--------------------------------------------------------------------===//
    bool isParent(common::table_id_t childID) override;
    common::TableType getTableType() const override { return common::TableType::REL_GROUP; }
    const std::vector<common::table_id_t>& getRelTableIDs() const { return relTableIDs; }

    //===--------------------------------------------------------------------===//
    // serialization & deserialization
    //===--------------------------------------------------------------------===//
    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<RelGroupCatalogEntry> deserialize(common::Deserializer& deserializer);
    std::string toCypher(main::ClientContext* clientContext) const override;
    std::unique_ptr<TableCatalogEntry> copy() const override;

private:
    std::unique_ptr<binder::BoundExtraCreateCatalogEntryInfo> getBoundExtraCreateInfo(
        transaction::Transaction* transaction) const override;

private:
    std::vector<common::table_id_t> relTableIDs;
};

} // namespace catalog
} // namespace kuzu
