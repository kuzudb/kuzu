#include "common/exception.h"

#include "common/string_utils.h"

namespace kuzu {
namespace common {

std::string ExceptionMessage::existedPKException(const std::string& pkString) {
    return StringUtils::string_format(
        "Found duplicated primary key value {}, which violates the uniqueness"
        " constraint of the primary key column.",
        pkString);
}

std::string ExceptionMessage::invalidPKType(const std::string& type) {
    return StringUtils::string_format(
        "Invalid primary key column type {}. Primary key must be either INT64, STRING or SERIAL.",
        type);
}

} // namespace common
} // namespace kuzu
