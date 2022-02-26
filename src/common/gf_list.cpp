#include "include/gf_list.h"

#include "include/gf_string.h"

#include "src/common/include/date.h"
#include "src/common/include/interval.h"
#include "src/common/include/timestamp.h"

namespace graphflow {
namespace common {

string gf_list_t::toString() const {
    string result = "[";
    for (auto i = 0u; i < size - 1; ++i) {
        result += elementToString(i) + ",";
    }
    result += elementToString(size - 1) + "]";
    return result;
}

// TODO: Refactor this function into TypeUtils after refactoring type.h. Together with the function
// in Value.cpp
string gf_list_t::elementToString(uint64_t pos) const {
    switch (childType) {
    case BOOL:
        return TypeUtils::toString(((bool*)overflowPtr)[pos]);
    case INT64:
        return TypeUtils::toString(((int64_t*)overflowPtr)[pos]);
    case DOUBLE:
        return TypeUtils::toString(((double_t*)overflowPtr)[pos]);
    case DATE:
        return Date::toString(((date_t*)overflowPtr)[pos]);
    case TIMESTAMP:
        return Timestamp::toString(((timestamp_t*)overflowPtr)[pos]);
    case INTERVAL:
        return Interval::toString(((interval_t*)overflowPtr)[pos]);
    default:
        assert(false);
    }
}

} // namespace common
} // namespace graphflow
