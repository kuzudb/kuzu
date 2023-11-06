#pragma once

#include "common/constants.h"
#include "table_schema.h"

namespace kuzu {
namespace catalog {

class NodeTableSchema : public TableSchema {
public:
    NodeTableSchema(common::property_id_t primaryPropertyId,
        std::unordered_set<common::table_id_t> fwdRelTableIDSet,
        std::unordered_set<common::table_id_t> bwdRelTableIDSet)
        : TableSchema{common::InternalKeyword::ANONYMOUS, common::INVALID_TABLE_ID,
              common::TableType::NODE, std::vector<std::unique_ptr<Property>>{}},
          primaryKeyPropertyID{primaryPropertyId}, fwdRelTableIDSet{std::move(fwdRelTableIDSet)},
          bwdRelTableIDSet{std::move(bwdRelTableIDSet)} {}
    NodeTableSchema(std::string tableName, common::table_id_t tableID,
        common::property_id_t primaryPropertyId, std::vector<std::unique_ptr<Property>> properties)
        : TableSchema{std::move(tableName), tableID, common::TableType::NODE,
              std::move(properties)},
          primaryKeyPropertyID{primaryPropertyId} {}
    NodeTableSchema(std::string tableName, common::table_id_t tableID,
        std::vector<std::unique_ptr<Property>> properties, std::string comment,
        common::property_id_t nextPropertyID, common::property_id_t primaryKeyPropertyID,
        std::unordered_set<common::table_id_t> fwdRelTableIDSet,
        std::unordered_set<common::table_id_t> bwdRelTableIDSet)
        : TableSchema{common::TableType::NODE, std::move(tableName), tableID, std::move(properties),
              std::move(comment), nextPropertyID},
          primaryKeyPropertyID{primaryKeyPropertyID}, fwdRelTableIDSet{std::move(fwdRelTableIDSet)},
          bwdRelTableIDSet{std::move(bwdRelTableIDSet)} {}

    inline void addFwdRelTableID(common::table_id_t tableID) { fwdRelTableIDSet.insert(tableID); }
    inline void addBwdRelTableID(common::table_id_t tableID) { bwdRelTableIDSet.insert(tableID); }

    inline Property* getPrimaryKey() const { return properties[primaryKeyPropertyID].get(); }

    static std::unique_ptr<NodeTableSchema> deserialize(common::Deserializer& deserializer);

    inline common::property_id_t getPrimaryKeyPropertyID() const { return primaryKeyPropertyID; }

    inline const std::unordered_set<common::table_id_t>& getFwdRelTableIDSet() const {
        return fwdRelTableIDSet;
    }

    inline const std::unordered_set<common::table_id_t>& getBwdRelTableIDSet() const {
        return bwdRelTableIDSet;
    }

    inline std::unique_ptr<TableSchema> copy() const override {
        return std::make_unique<NodeTableSchema>(tableName, tableID, Property::copy(properties),
            comment, nextPropertyID, primaryKeyPropertyID, fwdRelTableIDSet, bwdRelTableIDSet);
    }

private:
    void serializeInternal(common::Serializer& serializer) final;

private:
    // TODO(Semih): When we support updating the schemas, we need to update this or, we need
    // a more robust mechanism to keep track of which property is the primary key (e.g., store this
    // information with the property). This is an idx, not an ID, so as the columns/properties of
    // the table change, the idx can change.
    common::property_id_t primaryKeyPropertyID;
    std::unordered_set<common::table_id_t> fwdRelTableIDSet; // srcNode->rel
    std::unordered_set<common::table_id_t> bwdRelTableIDSet; // dstNode->rel
};

} // namespace catalog
} // namespace kuzu
