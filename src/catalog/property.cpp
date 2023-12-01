#include "catalog/property.h"

#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"

using namespace kuzu::common;

namespace kuzu {
namespace catalog {

void Property::serialize(Serializer& serializer) const {
    serializer.serializeValue(name);
    dataType->serialize(serializer);
    serializer.serializeValue(propertyID);
    serializer.serializeValue(tableID);
}

std::unique_ptr<Property> Property::deserialize(Deserializer& deserializer) {
    std::string name;
    property_id_t propertyID;
    table_id_t tableID;
    deserializer.deserializeValue(name);
    auto dataType = LogicalType::deserialize(deserializer);
    deserializer.deserializeValue(propertyID);
    deserializer.deserializeValue(tableID);
    return std::make_unique<Property>(name, std::move(dataType), propertyID, tableID);
}

std::vector<std::unique_ptr<Property>> Property::copy(
    const std::vector<std::unique_ptr<Property>>& properties) {
    std::vector<std::unique_ptr<Property>> result;
    result.reserve(properties.size());
    for (auto& property : properties) {
        result.push_back(property->copy());
    }
    return result;
}

} // namespace catalog
} // namespace kuzu
