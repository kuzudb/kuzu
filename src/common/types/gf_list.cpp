#include "include/gf_list.h"

#include <cassert>

namespace graphflow {
namespace common {

void gf_list_t::set(const uint8_t* values) const {
    memcpy(
        reinterpret_cast<uint8_t*>(overflowPtr), values, size * Types::getDataTypeSize(childType));
}

void gf_list_t::set(const gf_list_t& other) {
    this->childType = other.childType;
    this->size = other.size;
    set(reinterpret_cast<uint8_t*>(other.overflowPtr));
}

void gf_list_t::set(DataType childType, const vector<uint8_t*>& parameters) {
    this->childType = childType;
    this->size = parameters.size();
    auto numBytesOfListElement = Types::getDataTypeSize(childType);
    for (auto i = 0u; i < parameters.size(); i++) {
        memcpy(reinterpret_cast<uint8_t*>(this->overflowPtr) + (i * numBytesOfListElement),
            parameters[i], numBytesOfListElement);
    }
}

} // namespace common
} // namespace graphflow
