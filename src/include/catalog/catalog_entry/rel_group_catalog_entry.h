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
    RelGroupCatalogEntry(std::string tableName, common::table_id_t tableID,
        std::vector<common::table_id_t> relTableIDs);
    RelGroupCatalogEntry(const RelGroupCatalogEntry& other);

    //===--------------------------------------------------------------------===//
    // getter & setter
    //===--------------------------------------------------------------------===//
    bool isParent(common::table_id_t tableID) override;
    common::TableType getTableType() const override { return common::TableType::REL_GROUP; }
    std::vector<common::table_id_t>& getRelTableIDs() { return relTableIDs; }

    //===--------------------------------------------------------------------===//
    // serialization & deserialization
    //===--------------------------------------------------------------------===//
    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<RelGroupCatalogEntry> deserialize(common::Deserializer& deserializer);
    std::unique_ptr<CatalogEntry> copy() const override;

private:
    std::vector<common::table_id_t> relTableIDs;
};

} // namespace catalog
} // namespace kuzu
