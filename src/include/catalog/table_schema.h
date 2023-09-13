#pragma once

#include <algorithm>
#include <unordered_map>
#include <unordered_set>

#include "common/constants.h"
#include "common/table_type.h"
#include "property.h"

namespace kuzu {
namespace catalog {

class TableSchema {
public:
    TableSchema(std::string tableName, common::table_id_t tableID, common::TableType tableType,
        std::vector<std::unique_ptr<Property>> properties)
        : tableName{std::move(tableName)}, tableID{tableID}, tableType{tableType},
          properties{std::move(properties)},
          nextPropertyID{(common::property_id_t)this->properties.size()}, comment{
                                                                              std::move(comment)} {}
    TableSchema(common::TableType tableType, std::string tableName, common::table_id_t tableID,
        std::vector<std::unique_ptr<Property>> properties, std::string comment,
        common::property_id_t nextPropertyID)
        : tableType{tableType}, tableName{std::move(tableName)}, tableID{tableID},
          properties{std::move(properties)}, comment{std::move(comment)}, nextPropertyID{
                                                                              nextPropertyID} {}

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

    inline common::TableType getTableType() const { return tableType; }

    inline void updateTableName(std::string newTableName) { tableName = std::move(newTableName); }

    inline void setComment(std::string newComment) { comment = std::move(newComment); }

    virtual std::unique_ptr<TableSchema> copy() const = 0;

private:
    inline common::property_id_t increaseNextPropertyID() { return nextPropertyID++; }

    virtual void serializeInternal(common::FileInfo* fileInfo, uint64_t& offset) = 0;

public:
    common::TableType tableType;
    std::string tableName;
    common::table_id_t tableID;
    std::vector<std::unique_ptr<Property>> properties;
    std::string comment;
    common::property_id_t nextPropertyID;
};

} // namespace catalog
} // namespace kuzu
