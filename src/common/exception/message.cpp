#include "common/exception/message.h"

#include "common/string_format.h"

namespace kuzu {
namespace common {

std::string ExceptionMessage::existedPKException(const std::string& pkString) {
    return stringFormat("Found duplicated primary key value {}, which violates the uniqueness"
                        " constraint of the primary key column.",
        pkString);
}

std::string ExceptionMessage::nonExistPKException(const std::string& pkString) {
    return stringFormat("Found non-existed primary key value {}.", pkString);
}

std::string ExceptionMessage::invalidPKType(const std::string& type) {
    return stringFormat(
        "Invalid primary key column type {}. Primary key must be either INT64, STRING or SERIAL.",
        type);
}

std::string ExceptionMessage::overLargeStringPKValueException(uint64_t length) {
    return stringFormat("The maximum length of primary key strings is 4096 bytes. The input "
                        "string's length was {}.",
        length);
}
std::string ExceptionMessage::overLargeStringValueException(uint64_t length) {
    return stringFormat(
        "The maximum length of strings is 262144 bytes. The input string's length was {}.", length);
}

std::string ExceptionMessage::violateUniquenessOfRelAdjColumn(const std::string& tableName,
    const std::string& offset, const std::string& multiplicity, const std::string& direction) {
    return stringFormat("RelTable {} is a {} table, but node(nodeOffset: {}) "
                        "has more than one neighbour in the {} direction.",
        tableName, offset, multiplicity, direction);
}

std::string ExceptionMessage::validateCopyNpyNotForRelTablesException(
    const std::string& tableName) {
    return stringFormat("Copy from npy files to rel table {} is not supported yet.", tableName);
}

} // namespace common
} // namespace kuzu
