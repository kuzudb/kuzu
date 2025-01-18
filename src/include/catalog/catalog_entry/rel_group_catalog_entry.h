#pragma once

#include "table_catalog_entry.h"

namespace kuzu {
namespace catalog {

class RelGroupCatalogEntry final : public CatalogEntry {
    static constexpr CatalogEntryType type_ = CatalogEntryType::REL_GROUP_ENTRY;

public:
    RelGroupCatalogEntry() = default;
    RelGroupCatalogEntry(std::string tableName, CatalogSet* tables,
        std::vector<common::table_id_t> relTableIDs)
        : CatalogEntry{type_, std::move(tableName)}, tables{tables},
          relTableIDs{std::move(relTableIDs)} {}

    const std::vector<common::table_id_t>& getRelTableIDs() const { return relTableIDs; }

    bool isParent(common::table_id_t tableID) const;

    //===--------------------------------------------------------------------===//
    // serialization & deserialization
    //===--------------------------------------------------------------------===//
    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<RelGroupCatalogEntry> deserialize(common::Deserializer& deserializer);
    std::string toCypher(main::ClientContext* clientContext) const override;

    binder::BoundCreateTableInfo getBoundCreateTableInfo(transaction::Transaction* transaction,
        bool isInternal) const;

private:
    CatalogSet* tables = nullptr;
    std::vector<common::table_id_t> relTableIDs;
};

} // namespace catalog
} // namespace kuzu
