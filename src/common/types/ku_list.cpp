#include "common/types/ku_list.h"

#include <cassert>

namespace kuzu {
namespace common {

void ku_list_t::set(const uint8_t* values, const DataType& dataType) const {
    memcpy(reinterpret_cast<uint8_t*>(overflowPtr), values,
        size * Types::getDataTypeSize(*dataType.childType));
}

void ku_list_t::set(const std::vector<uint8_t*>& parameters, DataTypeID childTypeId) {
    this->size = parameters.size();
    auto numBytesOfListElement = Types::getDataTypeSize(childTypeId);
    for (auto i = 0u; i < parameters.size(); i++) {
        memcpy(reinterpret_cast<uint8_t*>(this->overflowPtr) + (i * numBytesOfListElement),
            parameters[i], numBytesOfListElement);
    }
}

} // namespace common
} // namespace kuzu
