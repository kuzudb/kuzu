#pragma once

#include "common/enums/extend_direction.h"
#include "common/enums/rel_direction.h"
#include "common/enums/rel_multiplicity.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "node_table_id_pair.h"

namespace kuzu {
namespace catalog {

struct RelGroupToCypherInfo : ToCypherInfo {
    const main::ClientContext* context;

    explicit RelGroupToCypherInfo(const main::ClientContext* context) : context{context} {}
};

struct RelTableCatalogInfo {
    NodeTableIDPair nodePair;
    common::oid_t oid = common::INVALID_OID;

    RelTableCatalogInfo() = default;
    RelTableCatalogInfo(NodeTableIDPair nodePair, common::oid_t oid)
        : nodePair{std::move(nodePair)}, oid{oid} {}

    void serialize(common::Serializer& ser) const;
    static RelTableCatalogInfo deserialize(common::Deserializer& deser);
};

class RelGroupCatalogEntry final : public TableCatalogEntry {
    static constexpr CatalogEntryType type_ = CatalogEntryType::REL_GROUP_ENTRY;

public:
    RelGroupCatalogEntry() = default;
    RelGroupCatalogEntry(std::string tableName, common::RelMultiplicity srcMultiplicity,
        common::RelMultiplicity dstMultiplicity, common::ExtendDirection storageDirection,
        std::vector<RelTableCatalogInfo> relTableInfos)
        : TableCatalogEntry{type_, std::move(tableName)}, srcMultiplicity{srcMultiplicity},
          dstMultiplicity{dstMultiplicity}, storageDirection{storageDirection},
          relTableInfos{std::move(relTableInfos)} {
        propertyCollection =
            PropertyDefinitionCollection{1}; // Skip NBR_NODE_ID column as the first one.
    }

    common::TableType getTableType() const override { return common::TableType::REL_GROUP; }

    common::RelMultiplicity getMultiplicity(common::RelDataDirection direction) const {
        return direction == common::RelDataDirection::FWD ? dstMultiplicity : srcMultiplicity;
    }

    common::idx_t getNumRelTables() const { return relTableInfos.size(); }
    const RelTableCatalogInfo& getSingleRelTableInfo() const ;
    const RelTableCatalogInfo* getRelTableInfo(common::table_id_t srcTableID, common::table_id_t dstTableID) const;

    std::unordered_set<common::table_id_t> getSrcNodeTableIDSet() const;
    std::unordered_set<common::table_id_t> getDstNodeTableIDSet() const;
    std::unordered_set<common::table_id_t> getBoundNodeTableIDSet(common::RelDataDirection direction) const {
        return direction == common::RelDataDirection::FWD ? getSrcNodeTableIDSet() : getDstNodeTableIDSet();
    }
    std::unordered_set<common::table_id_t> getNbrNodeTableIDSet(common::RelDataDirection direction) const {
        return direction == common::RelDataDirection::FWD ? getDstNodeTableIDSet() : getSrcNodeTableIDSet();
    }

    std::vector<common::RelDataDirection> getRelDataDirections() const;

    std::unique_ptr<TableCatalogEntry> alter(common::transaction_t timestamp, const binder::BoundAlterInfo &alterInfo) const override;

    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<RelGroupCatalogEntry> deserialize(common::Deserializer& deserializer);
    std::string toCypher(const ToCypherInfo& info) const override;

    std::unique_ptr<TableCatalogEntry> copy() const override;

protected:
    std::unique_ptr<binder::BoundExtraCreateCatalogEntryInfo> getBoundExtraCreateInfo(
        transaction::Transaction*) const override;

private:
    common::RelMultiplicity srcMultiplicity;
    common::RelMultiplicity dstMultiplicity;
    common::ExtendDirection storageDirection;
    std::vector<RelTableCatalogInfo> relTableInfos;
};

} // namespace catalog
} // namespace kuzu
