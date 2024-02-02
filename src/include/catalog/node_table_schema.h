#pragma once

#include "table_schema.h"

namespace kuzu {
namespace catalog {

class NodeTableSchema final : public TableSchema {
public:
    NodeTableSchema() : TableSchema{common::TableType::NODE} {}
    NodeTableSchema(
        std::string tableName, common::table_id_t tableID, common::property_id_t primaryKeyPID)
        : TableSchema{common::TableType::NODE, std::move(tableName), tableID}, primaryKeyPID{
                                                                                   primaryKeyPID} {}
    NodeTableSchema(const NodeTableSchema& other);

    inline bool isParent(common::table_id_t) override { return false; }

    inline const Property* getPrimaryKey() const { return &properties[primaryKeyPID]; }

    inline common::property_id_t getPrimaryKeyPropertyID() const { return primaryKeyPID; }

    inline void addFwdRelTableID(common::table_id_t tableID) { fwdRelTableIDSet.insert(tableID); }
    inline void addBwdRelTableID(common::table_id_t tableID) { bwdRelTableIDSet.insert(tableID); }
    inline const common::table_id_set_t& getFwdRelTableIDSet() const { return fwdRelTableIDSet; }
    inline const common::table_id_set_t& getBwdRelTableIDSet() const { return bwdRelTableIDSet; }

    inline std::unique_ptr<TableSchema> copy() const override {
        return std::make_unique<NodeTableSchema>(*this);
    }

    static std::unique_ptr<NodeTableSchema> deserialize(common::Deserializer& deserializer);

private:
    void serializeInternal(common::Serializer& serializer) final;

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
