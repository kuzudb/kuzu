#pragma once

#include "common/enums/rel_direction.h"
#include "common/enums/rel_multiplicity.h"
#include "table_catalog_entry.h"

namespace kuzu {
namespace catalog {

class RelTableCatalogEntry final : public TableCatalogEntry {
    static constexpr auto entryType_ = CatalogEntryType::REL_TABLE_ENTRY;

public:
    RelTableCatalogEntry()
        : srcMultiplicity{}, dstMultiplicity{}, srcTableID{common::INVALID_TABLE_ID},
          dstTableID{common::INVALID_TABLE_ID} {};
    RelTableCatalogEntry(CatalogSet* set, std::string name, common::RelMultiplicity srcMultiplicity,
        common::RelMultiplicity dstMultiplicity, common::table_id_t srcTableID,
        common::table_id_t dstTableID)
        : TableCatalogEntry{set, entryType_, std::move(name)}, srcMultiplicity{srcMultiplicity},
          dstMultiplicity{dstMultiplicity}, srcTableID{srcTableID}, dstTableID{dstTableID} {
        propertyCollection =
            PropertyDefinitionCollection{1}; // Skip NBR_NODE_ID column as the first one.
    }

    bool isParent(common::table_id_t tableID) override;
    common::TableType getTableType() const override { return common::TableType::REL; }
    common::table_id_t getSrcTableID() const { return srcTableID; }
    common::table_id_t getDstTableID() const { return dstTableID; }
    bool isSingleMultiplicity(common::RelDataDirection direction) const;
    common::RelMultiplicity getMultiplicity(common::RelDataDirection direction) const;
    common::table_id_t getBoundTableID(common::RelDataDirection relDirection) const;
    common::table_id_t getNbrTableID(common::RelDataDirection relDirection) const;

    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<RelTableCatalogEntry> deserialize(common::Deserializer& deserializer);

    std::unique_ptr<TableCatalogEntry> copy() const override;

    std::string toCypher(main::ClientContext* clientContext) const override;

private:
    std::unique_ptr<binder::BoundExtraCreateCatalogEntryInfo> getBoundExtraCreateInfo(
        transaction::Transaction* transaction) const override;

private:
    common::RelMultiplicity srcMultiplicity;
    common::RelMultiplicity dstMultiplicity;
    common::table_id_t srcTableID;
    common::table_id_t dstTableID;
};

} // namespace catalog
} // namespace kuzu
