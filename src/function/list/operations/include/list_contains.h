#pragma once

#include <cassert>
#include <cstring>

#include "list_position_operation.h"

#include "src/common/types/include/gf_list.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace function {
namespace operation {

struct ListContains {
    template<typename T>
    static inline void operation(gf_list_t& list, T& element, uint8_t& result, bool isListNull,
        bool isElementNull, const DataType& leftDataType, const DataType& rightDataType) {
        assert(!isListNull && !isElementNull);
        int64_t pos;
        ListPosition::operation(
            list, element, pos, isListNull, isElementNull, leftDataType, rightDataType);
        result = (pos != 0);
    }
};

} // namespace operation
} // namespace function
} // namespace graphflow
