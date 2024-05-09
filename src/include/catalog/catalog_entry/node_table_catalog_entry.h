#pragma once

#include "table_catalog_entry.h"

namespace kuzu {
namespace catalog {

class NodeTableCatalogEntry final : public TableCatalogEntry {
public:
    //===--------------------------------------------------------------------===//
    // constructors
    //===--------------------------------------------------------------------===//
    NodeTableCatalogEntry() = default;
    NodeTableCatalogEntry(CatalogSet* set, std::string name, common::table_id_t tableID,
        common::property_id_t primaryKeyPID);
    NodeTableCatalogEntry(const NodeTableCatalogEntry& other);

    //===--------------------------------------------------------------------===//
    // getter & setter
    //===--------------------------------------------------------------------===//
    bool isParent(common::table_id_t /*tableID*/) override { return false; }
    common::TableType getTableType() const override { return common::TableType::NODE; }
    const Property* getPrimaryKey() const { return getProperty(primaryKeyPID); }
    common::property_id_t getPrimaryKeyPID() const { return primaryKeyPID; }
    void addFwdRelTableID(common::table_id_t tableID) { fwdRelTableIDSet.insert(tableID); }
    void addBWdRelTableID(common::table_id_t tableID) { bwdRelTableIDSet.insert(tableID); }
    const common::table_id_set_t& getFwdRelTableIDSet() const { return fwdRelTableIDSet; }
    const common::table_id_set_t& getBwdRelTableIDSet() const { return bwdRelTableIDSet; }

    //===--------------------------------------------------------------------===//
    // serialization & deserialization
    //===--------------------------------------------------------------------===//
    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<NodeTableCatalogEntry> deserialize(common::Deserializer& deserializer);
    std::unique_ptr<TableCatalogEntry> copy() const override;
    std::string toCypher(main::ClientContext* /*clientContext*/) const override;

private:
    std::unique_ptr<binder::BoundExtraCreateCatalogEntryInfo> getBoundExtraCreateInfo(
        transaction::Transaction* transaction) const override;

private:
    // TODO(Semih): When we support updating the schemas, we need to update this or, we need
    // a more robust mechanism to keep track of which property is the primary key (e.g., store this
    // information with the property). This is an idx, not an ID, so as the columns/properties of
    // the table change, the idx can change.
    common::property_id_t primaryKeyPID;
    common::table_id_set_t fwdRelTableIDSet; // srcNode->rel
    common::table_id_set_t bwdRelTableIDSet; // dstNode->rel
};

} // namespace catalog
} // namespace kuzu
