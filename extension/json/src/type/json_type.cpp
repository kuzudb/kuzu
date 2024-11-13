#include "json_type.h"

#include "common/exception/binder.h"
#include "json_extension.h"

namespace kuzu {
namespace json_extension {

common::LogicalType JsonType::getJsonType() {
    auto type = common::LogicalType(common::LogicalTypeID::STRING, common::TypeCategory::UDT);
    type.setExtraTypeInfo(std::make_unique<common::UDTTypeInfo>(JsonExtension::JSON_TYPE_NAME));
    return type;
}

bool JsonType::isJson(const common::LogicalType& type) {
    if (!type.isInternalType() &&
        type.getExtraTypeInfo()->constPtrCast<common::UDTTypeInfo>()->getTypeName() ==
            JsonExtension::JSON_TYPE_NAME) {
        return true;
    }
    return false;
}

} // namespace json_extension
} // namespace kuzu
