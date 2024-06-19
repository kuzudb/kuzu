#include "catalog/property.h"

#include <sstream>

#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "common/string_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace catalog {

void Property::serialize(Serializer& serializer) const {
    serializer.serializeValue(name);
    dataType.serialize(serializer);
    defaultExpr->serialize(serializer);
    serializer.serializeValue(propertyID);
    serializer.serializeValue(columnID);
    serializer.serializeValue(tableID);
}

Property Property::deserialize(Deserializer& deserializer) {
    std::string name;
    property_id_t propertyID;
    column_id_t columnID;
    table_id_t tableID;
    deserializer.deserializeValue(name);
    auto dataType = LogicalType::deserialize(deserializer);
    auto defaultExpr = parser::ParsedExpression::deserialize(deserializer);
    deserializer.deserializeValue(propertyID);
    deserializer.deserializeValue(columnID);
    deserializer.deserializeValue(tableID);
    return Property(name, std::move(dataType), std::move(defaultExpr), propertyID, columnID,
        tableID);
}

std::string Property::toCypher(const std::vector<kuzu::catalog::Property>& properties) {
    std::stringstream ss;
    for (auto& prop : properties) {
        if (prop.getDataType().getPhysicalType() == PhysicalTypeID::INTERNAL_ID) {
            continue;
        }
        auto propStr = prop.getDataType().toString();
        StringUtils::replaceAll(propStr, ":", " ");
        if (propStr.find("MAP") != std::string::npos) {
            StringUtils::replaceAll(propStr, "  ", ",");
        }
        ss << prop.getName() << " " << propStr << ",";
    }
    return ss.str();
}

} // namespace catalog
} // namespace kuzu
