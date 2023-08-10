#pragma once

#include <algorithm>
#include <unordered_map>
#include <unordered_set>

#include "common/constants.h"
#include "common/exception.h"
#include "common/rel_direction.h"
#include "common/types/types_include.h"
#include "property.h"

namespace kuzu {
namespace storage {
class BMFileHandle;
}

namespace catalog {

enum class TableType : uint8_t { NODE = 0, REL = 1, INVALID = 2 };

enum class RelMultiplicity : uint8_t { MANY_MANY, MANY_ONE, ONE_MANY, ONE_ONE };
RelMultiplicity getRelMultiplicityFromString(const std::string& relMultiplicityString);
std::string getRelMultiplicityAsString(RelMultiplicity relMultiplicity);

class TableSchema {
public:
    TableSchema(std::string tableName, common::table_id_t tableID, TableType tableType,
        std::vector<std::unique_ptr<Property>> properties)
        : tableName{std::move(tableName)}, tableID{tableID}, tableType{tableType},
          properties{std::move(properties)}, nextPropertyID{
                                                 (common::property_id_t)this->properties.size()} {}
    TableSchema(TableType tableType, std::string tableName, common::table_id_t tableID,
        std::vector<std::unique_ptr<Property>> properties, common::property_id_t nextPropertyID)
        : tableType{tableType}, tableName{std::move(tableName)}, tableID{tableID},
          properties{std::move(properties)}, nextPropertyID{nextPropertyID} {}

    virtual ~TableSchema() = default;

    static bool isReservedPropertyName(const std::string& propertyName);

    inline uint32_t getNumProperties() const { return properties.size(); }

    inline void dropProperty(common::property_id_t propertyID) {
        properties.erase(std::remove_if(properties.begin(), properties.end(),
                             [propertyID](const std::unique_ptr<Property>& property) {
                                 return property->getPropertyID() == propertyID;
                             }),
            properties.end());
    }

    inline bool containProperty(std::string propertyName) const {
        return std::any_of(properties.begin(), properties.end(),
            [&propertyName](const std::unique_ptr<Property>& property) {
                return property->getName() == propertyName;
            });
    }

    std::vector<Property*> getProperties() const;

    inline void addNodeProperty(std::string propertyName,
        std::unique_ptr<common::LogicalType> dataType,
        std::unique_ptr<MetadataDAHInfo> metadataDAHInfo) {
        properties.push_back(std::make_unique<Property>(std::move(propertyName),
            std::move(dataType), increaseNextPropertyID(), tableID, std::move(metadataDAHInfo)));
    }
    inline void addRelProperty(
        std::string propertyName, std::unique_ptr<common::LogicalType> dataType) {
        properties.push_back(std::make_unique<Property>(
            std::move(propertyName), std::move(dataType), increaseNextPropertyID(), tableID));
    }

    std::string getPropertyName(common::property_id_t propertyID) const;

    common::property_id_t getPropertyID(const std::string& propertyName) const;

    Property* getProperty(common::property_id_t propertyID) const;

    void renameProperty(common::property_id_t propertyID, const std::string& newName);

    void serialize(common::FileInfo* fileInfo, uint64_t& offset);
    static std::unique_ptr<TableSchema> deserialize(common::FileInfo* fileInfo, uint64_t& offset);

    inline TableType getTableType() const { return tableType; }

    inline void updateTableName(std::string newTableName) { tableName = std::move(newTableName); }

    virtual std::unique_ptr<TableSchema> copy() const = 0;

protected:
    std::vector<std::unique_ptr<Property>> copyProperties() const;

private:
    inline common::property_id_t increaseNextPropertyID() { return nextPropertyID++; }

    virtual void serializeInternal(common::FileInfo* fileInfo, uint64_t& offset) = 0;

public:
    TableType tableType;
    std::string tableName;
    common::table_id_t tableID;
    std::vector<std::unique_ptr<Property>> properties;
    common::property_id_t nextPropertyID;
};

class NodeTableSchema : public TableSchema {
public:
    NodeTableSchema(common::property_id_t primaryPropertyId,
        std::unordered_set<common::table_id_t> fwdRelTableIDSet,
        std::unordered_set<common::table_id_t> bwdRelTableIDSet)
        : TableSchema{common::InternalKeyword::ANONYMOUS, common::INVALID_TABLE_ID, TableType::NODE,
              std::vector<std::unique_ptr<Property>>{}},
          primaryKeyPropertyID{primaryPropertyId}, fwdRelTableIDSet{std::move(fwdRelTableIDSet)},
          bwdRelTableIDSet{std::move(bwdRelTableIDSet)} {}
    NodeTableSchema(std::string tableName, common::table_id_t tableID,
        common::property_id_t primaryPropertyId, std::vector<std::unique_ptr<Property>> properties)
        : TableSchema{std::move(tableName), tableID, TableType::NODE, std::move(properties)},
          primaryKeyPropertyID{primaryPropertyId} {}
    NodeTableSchema(std::string tableName, common::table_id_t tableID,
        std::vector<std::unique_ptr<Property>> properties, common::property_id_t nextPropertyID,
        common::property_id_t primaryKeyPropertyID,
        std::unordered_set<common::table_id_t> fwdRelTableIDSet,
        std::unordered_set<common::table_id_t> bwdRelTableIDSet)
        : TableSchema{TableType::NODE, std::move(tableName), tableID, std::move(properties),
              nextPropertyID},
          primaryKeyPropertyID{primaryKeyPropertyID}, fwdRelTableIDSet{std::move(fwdRelTableIDSet)},
          bwdRelTableIDSet{std::move(bwdRelTableIDSet)} {}

    inline void addFwdRelTableID(common::table_id_t tableID) { fwdRelTableIDSet.insert(tableID); }
    inline void addBwdRelTableID(common::table_id_t tableID) { bwdRelTableIDSet.insert(tableID); }

    inline Property* getPrimaryKey() const { return properties[primaryKeyPropertyID].get(); }

    static std::unique_ptr<NodeTableSchema> deserialize(
        common::FileInfo* fileInfo, uint64_t& offset);

    inline common::property_id_t getPrimaryKeyPropertyID() const { return primaryKeyPropertyID; }

    inline const std::unordered_set<common::table_id_t>& getFwdRelTableIDSet() const {
        return fwdRelTableIDSet;
    }

    inline const std::unordered_set<common::table_id_t>& getBwdRelTableIDSet() const {
        return bwdRelTableIDSet;
    }

    inline std::unique_ptr<TableSchema> copy() const override {
        return std::make_unique<NodeTableSchema>(tableName, tableID, copyProperties(),
            nextPropertyID, primaryKeyPropertyID, fwdRelTableIDSet, bwdRelTableIDSet);
    }

private:
    void serializeInternal(common::FileInfo* fileInfo, uint64_t& offset) final;

private:
    // TODO(Semih): When we support updating the schemas, we need to update this or, we need
    // a more robust mechanism to keep track of which property is the primary key (e.g., store this
    // information with the property). This is an idx, not an ID, so as the columns/properties of
    // the table change, the idx can change.
    common::property_id_t primaryKeyPropertyID;
    std::unordered_set<common::table_id_t> fwdRelTableIDSet; // srcNode->rel
    std::unordered_set<common::table_id_t> bwdRelTableIDSet; // dstNode->rel
};

class RelTableSchema : public TableSchema {
public:
    static constexpr uint64_t INTERNAL_REL_ID_PROPERTY_ID = 0;

    RelTableSchema(RelMultiplicity relMultiplicity, common::table_id_t srcTableID,
        common::table_id_t dstTableID, std::unique_ptr<common::LogicalType> srcPKDataType,
        std::unique_ptr<common::LogicalType> dstPKDataType)
        : TableSchema{"", common::INVALID_TABLE_ID, TableType::REL, {} /* properties */},
          relMultiplicity{relMultiplicity}, srcTableID{srcTableID}, dstTableID{dstTableID},
          srcPKDataType{std::move(srcPKDataType)}, dstPKDataType{std::move(dstPKDataType)} {}
    RelTableSchema(std::string tableName, common::table_id_t tableID,
        RelMultiplicity relMultiplicity, std::vector<std::unique_ptr<Property>> properties,
        common::table_id_t srcTableID, common::table_id_t dstTableID,
        std::unique_ptr<common::LogicalType> srcPKDataType,
        std::unique_ptr<common::LogicalType> dstPKDataType)
        : TableSchema{std::move(tableName), tableID, TableType::REL, std::move(properties)},
          relMultiplicity{relMultiplicity}, srcTableID{srcTableID}, dstTableID{dstTableID},
          srcPKDataType{std::move(srcPKDataType)}, dstPKDataType{std::move(dstPKDataType)} {}
    RelTableSchema(std::string tableName, common::table_id_t tableID,
        std::vector<std::unique_ptr<Property>> properties, common::property_id_t nextPropertyID,
        RelMultiplicity relMultiplicity, common::table_id_t srcTableID,
        common::table_id_t dstTableID, std::unique_ptr<common::LogicalType> srcPKDataType,
        std::unique_ptr<common::LogicalType> dstPKDataType)
        : TableSchema{TableType::REL, std::move(tableName), tableID, std::move(properties),
              nextPropertyID},
          relMultiplicity{relMultiplicity}, srcTableID{srcTableID}, dstTableID{dstTableID},
          srcPKDataType{std::move(srcPKDataType)}, dstPKDataType{std::move(dstPKDataType)} {}

    inline bool isSingleMultiplicityInDirection(common::RelDataDirection direction) const {
        return relMultiplicity == RelMultiplicity::ONE_ONE ||
               relMultiplicity == (direction == common::RelDataDirection::FWD ?
                                          RelMultiplicity::MANY_ONE :
                                          RelMultiplicity::ONE_MANY);
    }

    inline bool isSrcOrDstTable(common::table_id_t tableID) const {
        return srcTableID == tableID || dstTableID == tableID;
    }

    inline common::table_id_t getBoundTableID(common::RelDataDirection relDirection) const {
        return relDirection == common::RelDataDirection::FWD ? srcTableID : dstTableID;
    }

    inline common::table_id_t getNbrTableID(common::RelDataDirection relDirection) const {
        return relDirection == common::RelDataDirection::FWD ? dstTableID : srcTableID;
    }

    static std::unique_ptr<RelTableSchema> deserialize(
        common::FileInfo* fileInfo, uint64_t& offset);

    inline std::unique_ptr<TableSchema> copy() const override {
        return std::make_unique<RelTableSchema>(tableName, tableID, copyProperties(),
            nextPropertyID, relMultiplicity, srcTableID, dstTableID, srcPKDataType->copy(),
            dstPKDataType->copy());
    }

    inline RelMultiplicity getRelMultiplicity() const { return relMultiplicity; }

    inline common::table_id_t getSrcTableID() const { return srcTableID; }

    inline common::table_id_t getDstTableID() const { return dstTableID; }

    inline common::LogicalType* getSrcPKDataType() const { return srcPKDataType.get(); }

    inline common::LogicalType* getDstPKDataType() const { return dstPKDataType.get(); }

private:
    void serializeInternal(common::FileInfo* fileInfo, uint64_t& offset) final;

private:
    RelMultiplicity relMultiplicity;
    common::table_id_t srcTableID;
    common::table_id_t dstTableID;
    std::unique_ptr<common::LogicalType> srcPKDataType;
    std::unique_ptr<common::LogicalType> dstPKDataType;
};

} // namespace catalog
} // namespace kuzu
