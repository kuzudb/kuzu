#pragma once

#include "common/copy_constructors.h"
#include "common/types/types.h"

namespace kuzu {
namespace common {
class Serializer;
class Deserializer;
} // namespace common
namespace catalog {

class Property {
public:
    Property(std::string name, std::unique_ptr<common::LogicalType> dataType)
        : Property{std::move(name), std::move(dataType), common::INVALID_PROPERTY_ID,
              common::INVALID_TABLE_ID} {}
    Property(std::string name, std::unique_ptr<common::LogicalType> dataType,
        common::property_id_t propertyID, common::table_id_t tableID)
        : name{std::move(name)}, dataType{std::move(dataType)},
          propertyID{propertyID}, tableID{tableID} {}
    EXPLICIT_COPY_DEFAULT_MOVE(Property);

    inline std::string getName() const { return name; }

    inline common::LogicalType* getDataType() const { return dataType.get(); }

    inline common::property_id_t getPropertyID() const { return propertyID; }

    inline common::table_id_t getTableID() const { return tableID; }

    inline void setPropertyID(common::property_id_t propertyID_) { this->propertyID = propertyID_; }

    inline void setTableID(common::table_id_t tableID_) { this->tableID = tableID_; }

    inline void rename(std::string newName) { name = std::move(newName); }

    void serialize(common::Serializer& serializer) const;
    static std::unique_ptr<Property> deserialize(common::Deserializer& deserializer);

    static std::vector<Property> copy(const std::vector<Property>& properties);

private:
    Property(const Property& other)
        : name{other.name}, dataType{other.dataType->copy()},
          propertyID{other.propertyID}, tableID{other.tableID} {}

private:
    std::string name;
    std::unique_ptr<common::LogicalType> dataType;
    common::property_id_t propertyID;
    common::table_id_t tableID;
};

} // namespace catalog
} // namespace kuzu
