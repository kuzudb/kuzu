#include "include/gf_list.h"

#include <cassert>

#include "include/type_utils.h"

namespace graphflow {
namespace common {

void gf_list_t::copyOverflow(const gf_list_t& other) {
    memcpy(overflowPtr, other.overflowPtr, size * TypeUtils::getDataTypeSize(childType));
}

string gf_list_t::toString() const {
    string result = "[";
    for (auto i = 0u; i < size - 1; ++i) {
        result += elementToString(i) + ",";
    }
    result += elementToString(size - 1) + "]";
    return result;
}

string gf_list_t::elementToString(uint64_t pos) const {
    switch (childType) {
    case BOOL:
        return TypeUtils::toString(((bool*)overflowPtr)[pos]);
    case INT64:
        return TypeUtils::toString(((int64_t*)overflowPtr)[pos]);
    case DOUBLE:
        return TypeUtils::toString(((double_t*)overflowPtr)[pos]);
    case DATE:
        return TypeUtils::toString(((date_t*)overflowPtr)[pos]);
    case TIMESTAMP:
        return TypeUtils::toString(((timestamp_t*)overflowPtr)[pos]);
    case INTERVAL:
        return TypeUtils::toString(((interval_t*)overflowPtr)[pos]);
    default:
        assert(false);
    }
}

} // namespace common
} // namespace graphflow
