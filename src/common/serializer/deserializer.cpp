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

void Deserializer::deserializeDebuggingInfo(std::string& value) {
#if defined(KUZU_RUNTIME_CHECKS) || !defined(NDEBUG)
    deserializeValue<std::string>(value);
#endif
    // DO NOTHING
    KU_UNUSED(value);
}

} // namespace common
} // namespace kuzu
