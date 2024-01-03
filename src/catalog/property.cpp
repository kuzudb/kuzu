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

Property Property::deserialize(Deserializer& deserializer) {
    std::string name;
    property_id_t propertyID;
    table_id_t tableID;
    deserializer.deserializeValue(name);
    auto dataType = LogicalType::deserialize(deserializer);
    deserializer.deserializeValue(propertyID);
    deserializer.deserializeValue(tableID);
    return Property(name, std::move(dataType), propertyID, tableID);
}

} // namespace catalog
} // namespace kuzu
