#pragma once

#include "table_catalog_entry.h"

namespace kuzu {
namespace catalog {

class Catalog;
class RelGroupCatalogEntry final : public CatalogEntry {
    static constexpr CatalogEntryType type_ = CatalogEntryType::REL_GROUP_ENTRY;

public:
    RelGroupCatalogEntry() = default;
    RelGroupCatalogEntry(std::string tableName, std::vector<common::table_id_t> relTableIDs)
        : CatalogEntry{type_, std::move(tableName)}, relTableIDs{std::move(relTableIDs)} {}

    common::idx_t getNumRelTables() const { return relTableIDs.size(); }
    const std::vector<common::table_id_t>& getRelTableIDs() const { return relTableIDs; }

    bool isParent(common::table_id_t tableID) const;

    //===--------------------------------------------------------------------===//
    // serialization & deserialization
    //===--------------------------------------------------------------------===//
    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<RelGroupCatalogEntry> deserialize(common::Deserializer& deserializer);
    std::string toCypher(main::ClientContext* clientContext) const override;

    binder::BoundCreateTableInfo getBoundCreateTableInfo(transaction::Transaction* transaction,
        Catalog* catalog, bool isInternal) const;

    static std::string getChildTableName(const std::string& groupName, const std::string& srcName,
        const std::string& dstName) {
        return groupName + "_" + srcName + "_" + dstName;
    }

private:
    std::vector<common::table_id_t> relTableIDs;
};

} // namespace catalog
} // namespace kuzu
