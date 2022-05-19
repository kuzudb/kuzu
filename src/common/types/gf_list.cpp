#include "include/gf_list.h"

#include <cassert>

namespace graphflow {
namespace common {

void gf_list_t::set(const uint8_t* values, const DataType& dataType) const {
    memcpy(reinterpret_cast<uint8_t*>(overflowPtr), values,
        size * Types::getDataTypeSize(*dataType.childType));
}

void gf_list_t::set(const vector<uint8_t*>& parameters, DataTypeID childTypeId) {
    this->size = parameters.size();
    auto numBytesOfListElement = Types::getDataTypeSize(childTypeId);
    for (auto i = 0u; i < parameters.size(); i++) {
        memcpy(reinterpret_cast<uint8_t*>(this->overflowPtr) + (i * numBytesOfListElement),
            parameters[i], numBytesOfListElement);
    }
}

} // namespace common
} // namespace graphflow
