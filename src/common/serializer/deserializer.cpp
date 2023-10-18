#include "common/serializer/deserializer.h"

namespace kuzu {
namespace common {

template<>
void Deserializer::deserializeValue(std::string& value) {
    uint64_t valueLength = 0;
    deserializeValue(valueLength);
    value.resize(valueLength);
    reader->read((uint8_t*)value.data(), valueLength);
}

} // namespace common
} // namespace kuzu
