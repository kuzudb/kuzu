#pragma once

#include <cassert>
#include <cstring>

#include "common/types/ku_list.h"

using namespace std;
using namespace kuzu::common;

namespace kuzu {
namespace function {
namespace operation {

struct ListPrepend {
    template<typename T>
    static inline void operation(
        T& list, ku_list_t& element, ku_list_t& result, ValueVector& resultValueVector) {
        auto elementSize = Types::getDataTypeSize(resultValueVector.dataType.childType->typeID);
        result.overflowPtr = reinterpret_cast<uint64_t>(
            resultValueVector.getOverflowBuffer().allocateSpace((element.size + 1) * elementSize));
        result.size = element.size + 1;
        ku_list_t tmpList;
        InMemOverflowBufferUtils::copyListRecursiveIfNested(
            element, tmpList, resultValueVector.dataType, resultValueVector.getOverflowBuffer());
        memcpy(reinterpret_cast<uint8_t*>(result.overflowPtr) + elementSize,
            reinterpret_cast<uint8_t*>(tmpList.overflowPtr), element.size * elementSize);
        InMemOverflowBufferUtils::setListElement(result, 0 /* elementPos */, list,
            resultValueVector.dataType, resultValueVector.getOverflowBuffer());
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
