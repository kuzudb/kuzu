#pragma once

#include "common/enums/table_type.h"
#include "property.h"

namespace kuzu {
namespace common {
class Serializer;
class Deserializer;
} // namespace common
namespace catalog {

class TableSchema {
public:
    TableSchema(std::string tableName, common::table_id_t tableID, common::TableType tableType,
        std::vector<std::unique_ptr<Property>> properties)
        : tableType{tableType}, tableName{std::move(tableName)}, tableID{tableID},
          properties{std::move(properties)}, comment{"" /* empty comment */},
          nextPropertyID{(common::property_id_t)this->properties.size()} {}
    TableSchema(common::TableType tableType, std::string tableName, common::table_id_t tableID,
        std::vector<std::unique_ptr<Property>> properties, std::string comment,
        common::property_id_t nextPropertyID)
        : tableType{tableType}, tableName{std::move(tableName)}, tableID{tableID},
          properties{std::move(properties)}, comment{std::move(comment)}, nextPropertyID{
                                                                              nextPropertyID} {}

    virtual ~TableSchema() = default;

    static bool isReservedPropertyName(const std::string& propertyName);

    inline common::table_id_t getTableID() const { return tableID; }

    inline uint32_t getNumProperties() const { return properties.size(); }
    std::vector<Property*> getProperties() const;
    bool containProperty(const std::string& propertyName) const;
    bool containsColumnType(const common::LogicalType& logicalType) const;
    void dropProperty(common::property_id_t propertyID);

    inline void addNodeProperty(
        std::string propertyName, std::unique_ptr<common::LogicalType> dataType) {
        properties.push_back(std::make_unique<Property>(
            std::move(propertyName), std::move(dataType), increaseNextPropertyID(), tableID));
    }
    inline void addRelProperty(
        std::string propertyName, std::unique_ptr<common::LogicalType> dataType) {
        properties.push_back(std::make_unique<Property>(
            std::move(propertyName), std::move(dataType), increaseNextPropertyID(), tableID));
    }

    common::property_id_t getPropertyID(const std::string& propertyName) const;
    common::column_id_t getColumnID(common::property_id_t propertyID) const;

    Property* getProperty(common::property_id_t propertyID) const;

    void renameProperty(common::property_id_t propertyID, const std::string& newName);

    void serialize(common::Serializer& serializer);
    static std::unique_ptr<TableSchema> deserialize(common::Deserializer& deserializer);

    inline common::TableType getTableType() const { return tableType; }

    inline void updateTableName(std::string newTableName) { tableName = std::move(newTableName); }

    inline void setComment(std::string newComment) { comment = std::move(newComment); }

    virtual std::unique_ptr<TableSchema> copy() const = 0;

private:
    inline common::property_id_t increaseNextPropertyID() { return nextPropertyID++; }

    virtual void serializeInternal(common::Serializer& serializer) = 0;

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
