#pragma once

#include <cassert>
#include <cstring>

#include "common/in_mem_overflow_buffer_utils.h"
#include "common/types/ku_list.h"
#include "common/vector/value_vector.h"

namespace kuzu {
namespace function {
namespace operation {

struct ListAppend {
    template<typename T>
    static inline void operation(common::ku_list_t& list, T& element, common::ku_list_t& result,
        common::ValueVector& resultValueVector) {
        auto elementSize = common::Types::getDataTypeSize(*resultValueVector.dataType.childType);
        result.overflowPtr = reinterpret_cast<uint64_t>(
            resultValueVector.getOverflowBuffer().allocateSpace((list.size + 1) * elementSize));
        result.size = list.size + 1;
        common::ku_list_t tmpList;
        common::InMemOverflowBufferUtils::copyListRecursiveIfNested(
            list, tmpList, resultValueVector.dataType, resultValueVector.getOverflowBuffer());
        memcpy(reinterpret_cast<uint8_t*>(result.overflowPtr),
            reinterpret_cast<uint8_t*>(tmpList.overflowPtr), list.size * elementSize);
        common::InMemOverflowBufferUtils::setListElement(result, list.size, element,
            resultValueVector.dataType, resultValueVector.getOverflowBuffer());
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
