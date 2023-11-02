#pragma once

#include "common/vector/value_vector.h"
#include "list_position_function.h"

namespace kuzu {
namespace function {

struct ListContains {
    template<typename T>
    static inline void operation(common::list_entry_t& list, T& element, uint8_t& result,
        common::ValueVector& listVector, common::ValueVector& elementVector,
        common::ValueVector& resultVector) {
        int64_t pos;
        ListPosition::operation(list, element, pos, listVector, elementVector, resultVector);
        result = (pos != 0);
    }
};

} // namespace function
} // namespace kuzu
