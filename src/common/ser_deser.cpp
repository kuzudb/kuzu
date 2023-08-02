#include "common/ser_deser.h"

namespace kuzu {
namespace common {

template<>
void SerDeser::serializeValue<std::string>(
    const std::string& value, FileInfo* fileInfo, uint64_t& offset) {
    uint64_t valueLength = value.length();
    FileUtils::writeToFile(fileInfo, (uint8_t*)&valueLength, sizeof(uint64_t), offset);
    FileUtils::writeToFile(
        fileInfo, (uint8_t*)value.data(), valueLength, offset + sizeof(uint64_t));
    offset = offset + sizeof(uint64_t) + valueLength;
}

template<>
void SerDeser::deserializeValue<std::string>(
    std::string& value, FileInfo* fileInfo, uint64_t& offset) {
    uint64_t valueLength = 0;
    deserializeValue(valueLength, fileInfo, offset);
    value.resize(valueLength);
    FileUtils::readFromFile(fileInfo, (uint8_t*)value.data(), valueLength, offset);
    offset += valueLength;
}

} // namespace common
} // namespace kuzu
