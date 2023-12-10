#pragma once

#include "table_schema.h"

namespace kuzu {
namespace catalog {

class NodeTableSchema : public TableSchema {
public:
    NodeTableSchema() : TableSchema{common::TableType::NODE} {}
    NodeTableSchema(std::string tableName, common::table_id_t tableID,
        common::property_id_t primaryPropertyId, property_vector_t properties)
        : TableSchema{std::move(tableName), tableID, common::TableType::NODE,
              std::move(properties)},
          primaryKeyPropertyID{primaryPropertyId} {}
    NodeTableSchema(const NodeTableSchema& other);

    // TODO(Xiyang): this seems in correct;
    inline Property* getPrimaryKey() const { return properties[primaryKeyPropertyID].get(); }

    static std::unique_ptr<NodeTableSchema> deserialize(common::Deserializer& deserializer);

    inline common::property_id_t getPrimaryKeyPropertyID() const { return primaryKeyPropertyID; }

    inline void addFwdRelTableID(common::table_id_t tableID) { fwdRelTableIDSet.insert(tableID); }
    inline void addBwdRelTableID(common::table_id_t tableID) { bwdRelTableIDSet.insert(tableID); }
    inline const common::table_id_set_t& getFwdRelTableIDSet() const { return fwdRelTableIDSet; }
    inline const common::table_id_set_t& getBwdRelTableIDSet() const { return bwdRelTableIDSet; }

    inline std::unique_ptr<TableSchema> copy() const override {
        return std::make_unique<NodeTableSchema>(*this);
    }

private:
    void serializeInternal(common::Serializer& serializer) final;

private:
    // TODO(Semih): When we support updating the schemas, we need to update this or, we need
    // a more robust mechanism to keep track of which property is the primary key (e.g., store this
    // information with the property). This is an idx, not an ID, so as the columns/properties of
    // the table change, the idx can change.
    common::property_id_t primaryKeyPropertyID;
    common::table_id_set_t fwdRelTableIDSet; // srcNode->rel
    common::table_id_set_t bwdRelTableIDSet; // dstNode->rel
};

} // namespace catalog
} // namespace kuzu
