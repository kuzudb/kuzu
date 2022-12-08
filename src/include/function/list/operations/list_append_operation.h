#pragma once

#include <cassert>
#include <cstring>

#include "common/in_mem_overflow_buffer_utils.h"
#include "common/types/ku_list.h"
#include "common/vector/value_vector.h"

using namespace std;
using namespace kuzu::common;

namespace kuzu {
namespace function {
namespace operation {

struct ListAppend {
    template<typename T>
    static inline void operation(
        ku_list_t& list, T& element, ku_list_t& result, ValueVector& resultValueVector) {
        auto elementSize = Types::getDataTypeSize(*resultValueVector.dataType.childType);
        result.overflowPtr = reinterpret_cast<uint64_t>(
            resultValueVector.getOverflowBuffer().allocateSpace((list.size + 1) * elementSize));
        result.size = list.size + 1;
        ku_list_t tmpList;
        InMemOverflowBufferUtils::copyListRecursiveIfNested(
            list, tmpList, resultValueVector.dataType, resultValueVector.getOverflowBuffer());
        memcpy(reinterpret_cast<uint8_t*>(result.overflowPtr),
            reinterpret_cast<uint8_t*>(tmpList.overflowPtr), list.size * elementSize);
        InMemOverflowBufferUtils::setListElement(result, list.size, element,
            resultValueVector.dataType, resultValueVector.getOverflowBuffer());
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
