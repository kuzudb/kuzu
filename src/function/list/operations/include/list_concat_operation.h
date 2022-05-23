#pragma once

#include <cassert>
#include <cstring>

#include "src/common/types/include/gf_list.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace function {
namespace operation {

struct ListConcat {
public:
    static inline void operation(gf_list_t& left, gf_list_t& right, gf_list_t& result,
        bool isLeftNull, bool isRightNull, ValueVector& resultValueVector) {
        assert(!isLeftNull && !isRightNull);
        auto elementSize = Types::getDataTypeSize(resultValueVector.dataType.childType->typeID);
        result.overflowPtr =
            reinterpret_cast<uint64_t>(resultValueVector.getOverflowBuffer().allocateSpace(
                (left.size + right.size) * elementSize));
        gf_list_t tmpList1, tmpList2;
        OverflowBufferUtils::copyListRecursiveIfNested(
            left, tmpList1, resultValueVector.dataType, resultValueVector.getOverflowBuffer());
        OverflowBufferUtils::copyListRecursiveIfNested(
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
} // namespace graphflow
