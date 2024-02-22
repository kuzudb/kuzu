#pragma once

#include <vector>

#include "catalog/property.h"
#include "catalog_entry.h"
#include "common/enums/table_type.h"

namespace kuzu {
namespace catalog {

class TableCatalogEntry : public CatalogEntry {
public:
    //===--------------------------------------------------------------------===//
    // constructors
    //===--------------------------------------------------------------------===//
    TableCatalogEntry() = default;
    TableCatalogEntry(CatalogEntryType catalogType, std::string name, common::table_id_t tableID)
        : CatalogEntry{catalogType, std::move(name)}, tableID{tableID}, nextPID{0} {}
    TableCatalogEntry(const TableCatalogEntry& other)
        : CatalogEntry{other}, tableID{other.tableID}, comment{other.comment},
          nextPID{other.nextPID}, properties{other.properties} {}

    //===--------------------------------------------------------------------===//
    // getter & setter
    //===--------------------------------------------------------------------===//
    common::table_id_t getTableID() const { return tableID; }
    std::string getComment() const { return comment; }
    void setComment(std::string newComment) { comment = std::move(newComment); }
    virtual bool isParent(common::table_id_t tableID) = 0;
    virtual common::TableType getTableType() const = 0;

    //===--------------------------------------------------------------------===//
    // properties functions
    //===--------------------------------------------------------------------===//
    uint32_t getNumProperties() const { return properties.size(); }
    const std::vector<Property>& getPropertiesRef() const { return properties; }
    bool containProperty(const std::string& propertyName) const;
    common::property_id_t getPropertyID(const std::string& propertyName) const;
    const Property* getProperty(common::property_id_t propertyID) const;
    common::column_id_t getColumnID(common::property_id_t propertyID) const;
    bool containPropertyType(const common::LogicalType& logicalType) const;
    void addProperty(std::string propertyName, std::unique_ptr<common::LogicalType> dataType);
    void dropProperty(common::property_id_t propertyID);
    void renameProperty(common::property_id_t propertyID, const std::string& newName);

    //===--------------------------------------------------------------------===//
    // serialization & deserialization
    //===--------------------------------------------------------------------===//
    void serialize(common::Serializer& serializer) const override;
    static std::unique_ptr<TableCatalogEntry> deserialize(
        common::Deserializer& deserializer, CatalogEntryType type);

private:
    common::table_id_t tableID;
    std::string comment;
    common::property_id_t nextPID;
    std::vector<Property> properties;
};

} // namespace catalog
} // namespace kuzu
