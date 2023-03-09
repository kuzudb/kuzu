#pragma once

#include <cassert>
#include <cstring>

#include "common/types/ku_list.h"

namespace kuzu {
namespace function {
namespace operation {

struct ListPrepend {
    template<typename T>
    static inline void operation(T& list, common::ku_list_t& element, common::ku_list_t& result,
        common::ValueVector& resultValueVector) {
        auto elementSize =
            common::Types::getDataTypeSize(resultValueVector.dataType.childType->typeID);
        result.overflowPtr = reinterpret_cast<uint64_t>(
            resultValueVector.getOverflowBuffer().allocateSpace((element.size + 1) * elementSize));
        result.size = element.size + 1;
        common::ku_list_t tmpList;
        common::InMemOverflowBufferUtils::copyListRecursiveIfNested(
            element, tmpList, resultValueVector.dataType, resultValueVector.getOverflowBuffer());
        memcpy(reinterpret_cast<uint8_t*>(result.overflowPtr) + elementSize,
            reinterpret_cast<uint8_t*>(tmpList.overflowPtr), element.size * elementSize);
        common::InMemOverflowBufferUtils::setListElement(result, 0 /* elementPos */, list,
            resultValueVector.dataType, resultValueVector.getOverflowBuffer());
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
