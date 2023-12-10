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
    explicit TableSchema(common::TableType tableType)
        : tableType{tableType}, tableName{}, tableID{common::INVALID_TABLE_ID}, comment{},
          nextPropertyID{0} {}
    TableSchema(std::string tableName, common::table_id_t tableID, common::TableType tableType,
        std::vector<std::unique_ptr<Property>> properties)
        : tableType{tableType}, tableName{std::move(tableName)}, tableID{tableID},
          properties{std::move(properties)}, comment{},
          nextPropertyID{(common::property_id_t)this->properties.size()} {}
    TableSchema(const TableSchema& other);

    virtual ~TableSchema() = default;

    /* Table functions */
    inline common::table_id_t getTableID() const { return tableID; }
    inline common::TableType getTableType() const { return tableType; }
    inline void renameTable(std::string newName) { tableName = std::move(newName); }

    inline void setComment(std::string newComment) { comment = std::move(newComment); }

    /* Property functions */
    static bool isReservedPropertyName(const std::string& propertyName);

    inline uint32_t getNumProperties() const { return properties.size(); }
    std::vector<Property*> getProperties() const;
    bool containProperty(const std::string& propertyName) const;
    bool containPropertyType(const common::LogicalType& logicalType) const;
    common::property_id_t getPropertyID(const std::string& propertyName) const;
    Property* getProperty(common::property_id_t propertyID) const;

    common::column_id_t getColumnID(common::property_id_t propertyID) const;

    void addProperty(std::string propertyName, std::unique_ptr<common::LogicalType> dataType);
    void dropProperty(common::property_id_t propertyID);
    void renameProperty(common::property_id_t propertyID, const std::string& newName);

    void serialize(common::Serializer& serializer);
    static std::unique_ptr<TableSchema> deserialize(common::Deserializer& deserializer);

    virtual std::unique_ptr<TableSchema> copy() const = 0;

private:
    virtual void serializeInternal(common::Serializer& serializer) = 0;

public:
    common::TableType tableType;
    std::string tableName;
    common::table_id_t tableID;
    property_vector_t properties;
    std::string comment;
    common::property_id_t nextPropertyID;
};

} // namespace catalog
} // namespace kuzu
