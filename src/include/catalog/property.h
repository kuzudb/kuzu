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
    Property() = default;
    Property(std::string name, std::unique_ptr<common::LogicalType> dataType)
        : Property{std::move(name), std::move(dataType), common::INVALID_PROPERTY_ID,
              common::INVALID_COLUMN_ID, common::INVALID_TABLE_ID} {}
    Property(std::string name, std::unique_ptr<common::LogicalType> dataType,
        common::property_id_t propertyID, common::column_id_t columnID, common::table_id_t tableID)
        : name{std::move(name)}, dataType{std::move(dataType)}, propertyID{propertyID},
          columnID{columnID}, tableID{tableID} {}
    EXPLICIT_COPY_DEFAULT_MOVE(Property);

    std::string getName() const { return name; }

    const common::LogicalType* getDataType() const { return dataType.get(); }
    common::property_id_t getPropertyID() const { return propertyID; }
    common::column_id_t getColumnID() const { return columnID; }
    common::table_id_t getTableID() const { return tableID; }

    void rename(std::string newName) { name = std::move(newName); }

    void serialize(common::Serializer& serializer) const;
    static Property deserialize(common::Deserializer& deserializer);

    static std::string toCypher(const std::vector<kuzu::catalog::Property>& properties);

private:
    Property(const Property& other)
        : name{other.name}, dataType{other.dataType->copy()}, propertyID{other.propertyID},
          columnID{other.columnID}, tableID{other.tableID} {}

private:
    std::string name;
    std::unique_ptr<common::LogicalType> dataType;
    common::property_id_t propertyID;
    common::column_id_t columnID;
    common::table_id_t tableID;
};

} // namespace catalog
} // namespace kuzu
