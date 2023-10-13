#include "common/serializer/serializer.h"

namespace kuzu {
namespace common {

template<>
void Serializer::serializeValue(const std::string& value) {
    uint64_t valueLength = value.length();
    writer->write((uint8_t*)&valueLength, sizeof(uint64_t));
    writer->write((uint8_t*)value.data(), valueLength);
}

} // namespace common
} // namespace kuzu
