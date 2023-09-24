#include "catalog/property.h"

#include "common/ser_deser.h"

using namespace kuzu::common;

namespace kuzu {
namespace catalog {

void Property::serialize(FileInfo* fileInfo, uint64_t& offset) const {
    SerDeser::serializeValue(name, fileInfo, offset);
    dataType->serialize(fileInfo, offset);
    SerDeser::serializeValue(propertyID, fileInfo, offset);
    SerDeser::serializeValue(tableID, fileInfo, offset);
}

std::unique_ptr<Property> Property::deserialize(FileInfo* fileInfo, uint64_t& offset) {
    std::string name;
    property_id_t propertyID;
    table_id_t tableID;
    SerDeser::deserializeValue(name, fileInfo, offset);
    auto dataType = LogicalType::deserialize(fileInfo, offset);
    SerDeser::deserializeValue(propertyID, fileInfo, offset);
    SerDeser::deserializeValue(tableID, fileInfo, offset);
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
