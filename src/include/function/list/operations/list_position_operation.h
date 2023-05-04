#pragma once

#include <cassert>
#include <cstring>

#include "common/types/ku_list.h"

namespace kuzu {
namespace function {
namespace operation {

struct ListPosition {
    // Note: this function takes in a 1-based element (The index of the first element in the list
    // is 1).
    template<typename T>
    static inline void operation(common::list_entry_t& list, T& element, int64_t& result,
        common::ValueVector& listVector, common::ValueVector& elementVector,
        common::ValueVector& resultVector) {
        if (*listVector.dataType.getChildType() != elementVector.dataType) {
            result = 0;
            return;
        }
        auto listElements =
            reinterpret_cast<T*>(common::ListVector::getListValues(&listVector, list));
        for (auto i = 0u; i < list.size; i++) {
            if (common::TypeUtils::isValueEqual(listElements[i], element, nullptr /* leftVector */,
                    nullptr /* rightVector */)) {
                result = i + 1;
                return;
            }
        }
        result = 0;
    }
};

template<>
void ListPosition::operation(common::list_entry_t& list, common::list_entry_t& element,
    int64_t& result, common::ValueVector& listVector, common::ValueVector& elementVector,
    common::ValueVector& resultVector) {
    if (*listVector.dataType.getChildType() != elementVector.dataType) {
        result = 0;
        return;
    }
    auto listElements = reinterpret_cast<common::list_entry_t*>(
        common::ListVector::getListValues(&listVector, list));
    for (auto i = 0u; i < list.size; i++) {
        if (common::TypeUtils::isValueEqual(listElements[i], element,
                common::ListVector::getDataVector(&listVector), &elementVector)) {
            result = i + 1;
            return;
        }
    }
    result = 0;
}

} // namespace operation
} // namespace function
} // namespace kuzu
