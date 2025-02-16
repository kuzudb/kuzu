#pragma once

#include "table_catalog_entry.h"

namespace kuzu {
namespace catalog {

struct RelGroupToCypherInfo : public ToCypherInfo {
    const main::ClientContext* context;

    explicit RelGroupToCypherInfo(const main::ClientContext* context) : context{context} {}
};

class Catalog;
class RelGroupCatalogEntry final : public CatalogEntry {
    static constexpr CatalogEntryType type_ = CatalogEntryType::REL_GROUP_ENTRY;

public:
    RelGroupCatalogEntry() = default;
    RelGroupCatalogEntry(std::string tableName, std::vector<common::table_id_t> relTableIDs)
        : CatalogEntry{type_, std::move(tableName)}, relTableIDs{std::move(relTableIDs)} {}

    common::idx_t getNumRelTables() const { return relTableIDs.size(); }
    const std::vector<common::table_id_t>& getRelTableIDs() const { return relTableIDs; }

    std::unique_ptr<RelGroupCatalogEntry> alter(common::transaction_t timestamp,
        const binder::BoundAlterInfo& alterInfo) const;

    bool isParent(common::table_id_t tableID) const;

    //===--------------------------------------------------------------------===//
    // serialization & deserialization
    //===--------------------------------------------------------------------===//
    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<RelGroupCatalogEntry> deserialize(common::Deserializer& deserializer);
    std::string toCypher(const ToCypherInfo& info) const override;

    binder::BoundCreateTableInfo getBoundCreateTableInfo(transaction::Transaction* transaction,
        const Catalog* catalog, bool isInternal) const;

    static std::string getChildTableName(const std::string& groupName, const std::string& srcName,
        const std::string& dstName) {
        return groupName + "_" + srcName + "_" + dstName;
    }

    void setComment(std::string newComment) { comment = std::move(newComment); }
    std::string getComment() const { return comment; }

    std::unique_ptr<RelGroupCatalogEntry> copy() const {
        return std::make_unique<RelGroupCatalogEntry>(getName(), relTableIDs);
    }

private:
    std::vector<common::table_id_t> relTableIDs;
    std::string comment;
};

} // namespace catalog
} // namespace kuzu
