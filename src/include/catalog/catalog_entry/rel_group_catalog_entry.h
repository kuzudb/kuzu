#pragma once

#include "common/enums/extend_direction.h"
#include "common/enums/rel_direction.h"
#include "table_catalog_entry.h"

namespace kuzu {
namespace catalog {

struct RelGroupToCypherInfo : public ToCypherInfo {
    const main::ClientContext* context;

    explicit RelGroupToCypherInfo(const main::ClientContext* context) : context{context} {}
};

struct RelTableInfo {
    common::NodePair nodePair;
    common::oid_t oid = common::INVALID_OID;

    RelTableInfo() = default;
    RelTableInfo(common::NodePair nodePair, common::oid_t oid)
        : nodePair{std::move(nodePair)}, oid{oid} {}

    void serialize(common::Serializer& ser) const;
    static RelTableInfo deserialize(common::Deserializer& deser);
};

class Catalog;
class RelGroupCatalogEntry final : public TableCatalogEntry {
    static constexpr CatalogEntryType type_ = CatalogEntryType::REL_GROUP_ENTRY;

public:
    RelGroupCatalogEntry() = default;
    RelGroupCatalogEntry(std::string tableName, common::RelMultiplicity srcMultiplicity,
        common::RelMultiplicity dstMultiplicity, common::ExtendDirection storageDirection,
        std::vector<RelTableInfo> relTableInfos)
        : TableCatalogEntry{type_, std::move(tableName)}, srcMultiplicity{srcMultiplicity},
          dstMultiplicity{dstMultiplicity}, storageDirection{storageDirection},
          relTableInfos{std::move(relTableInfos)} {
        propertyCollection =
            PropertyDefinitionCollection{1}; // Skip NBR_NODE_ID column as the first one.
    }

    common::TableType getTableType() const override { return common::TableType::REL_GROUP; }

    common::idx_t getNumRelTables() const { return relTableInfos.size(); }
    std::vector<common::table_id_t> getRelTableIDs() const {
        std::vector<common::table_id_t> tableIDs;
        for (auto& relTableInfo : relTableInfos) {
            tableIDs.push_back(relTableInfo.oid);
        }
        return tableIDs;
    }

    //===--------------------------------------------------------------------===//
    // serialization & deserialization
    //===--------------------------------------------------------------------===//
    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<RelGroupCatalogEntry> deserialize(common::Deserializer& deserializer);
    std::string toCypher(const ToCypherInfo& info) const override;

    static std::string getChildTableName(const std::string& groupName, const std::string& srcName,
        const std::string& dstName) {
        return groupName + "_" + srcName + "_" + dstName;
    }

    void setComment(std::string newComment) { comment = std::move(newComment); }
    std::string getComment() const { return comment; }

    const std::vector<RelTableInfo>& getRelTableInfos() const { return relTableInfos; }

    std::vector<common::RelDataDirection> getRelDataDirections() const;

    // TODO(Ziyi relgroup): remove those two apis
    common::table_id_t getSrcTableID() const { return relTableInfos[0].nodePair.srcTableID; }

    common::table_id_t getDstTableID() const { return relTableInfos[0].nodePair.dstTableID; }

    std::unique_ptr<TableCatalogEntry> copy() const override {
        auto other = std::make_unique<RelGroupCatalogEntry>();
        other->srcMultiplicity = srcMultiplicity;
        other->dstMultiplicity = dstMultiplicity;
        other->storageDirection = storageDirection;
        other->relTableInfos = relTableInfos;
        other->copyFrom(*this);
        return other;
    }

protected:
    std::unique_ptr<binder::BoundExtraCreateCatalogEntryInfo> getBoundExtraCreateInfo(
        transaction::Transaction*) const override;

private:
    common::RelMultiplicity srcMultiplicity;
    common::RelMultiplicity dstMultiplicity;
    common::ExtendDirection storageDirection;
    std::vector<RelTableInfo> relTableInfos;
};

} // namespace catalog
} // namespace kuzu
