#pragma once

#include <cassert>
#include <cstring>

#include "common/types/ku_list.h"

using namespace std;
using namespace kuzu::common;

namespace kuzu {
namespace function {
namespace operation {

struct ListConcat {
public:
    static inline void operation(
        ku_list_t& left, ku_list_t& right, ku_list_t& result, ValueVector& resultValueVector) {
        auto elementSize = Types::getDataTypeSize(resultValueVector.dataType.childType->typeID);
        result.overflowPtr =
            reinterpret_cast<uint64_t>(resultValueVector.getOverflowBuffer().allocateSpace(
                (left.size + right.size) * elementSize));
        ku_list_t tmpList1, tmpList2;
        InMemOverflowBufferUtils::copyListRecursiveIfNested(
            left, tmpList1, resultValueVector.dataType, resultValueVector.getOverflowBuffer());
        InMemOverflowBufferUtils::copyListRecursiveIfNested(
            right, tmpList2, resultValueVector.dataType, resultValueVector.getOverflowBuffer());
        memcpy(reinterpret_cast<uint8_t*>(result.overflowPtr),
            reinterpret_cast<uint8_t*>(tmpList1.overflowPtr), left.size * elementSize);
        memcpy(reinterpret_cast<uint8_t*>(result.overflowPtr) + left.size * elementSize,
            reinterpret_cast<uint8_t*>(tmpList2.overflowPtr), right.size * elementSize);
        result.size = left.size + right.size;
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
