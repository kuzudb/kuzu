#include "json_type.h"

#include "common/exception/binder.h"
#include "json_extension.h"

namespace kuzu {
namespace json_extension {

void JsonType::validate(const common::LogicalType& type) {
    if (type.isInternalType() ||
        type.getExtraTypeInfo()->constPtrCast<common::UDTTypeInfo>()->getTypeName() !=
            JsonExtension::JSON_TYPE_NAME) {
        throw common::BinderException("Expected JSON as parameter type. Got: " + type.toString());
    }
}

common::LogicalType JsonType::getJsonType() {
    auto type = common::LogicalType(common::LogicalTypeID::STRING, common::TypeInfo::UDT);
    type.setExtraTypeInfo(std::make_unique<common::UDTTypeInfo>(JsonExtension::JSON_TYPE_NAME));
    return type;
}

} // namespace json_extension
} // namespace kuzu
