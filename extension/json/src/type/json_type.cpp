#include "json_type.h"

#include "common/exception/binder.h"
#include "json_extension.h"

namespace kuzu {
namespace json_extension {

common::LogicalType JsonType::getJsonType() {
    auto type = common::LogicalType(common::LogicalTypeID::STRING, common::TypeInfo::UDT);
    type.setExtraTypeInfo(std::make_unique<common::UDTTypeInfo>(JsonExtension::JSON_TYPE_NAME));
    return type;
}

} // namespace json_extension
} // namespace kuzu
