#pragma once

#include <cassert>
#include <cstring>

#include "base_pad_operation.h"
#include "common/types/ku_string.h"

using namespace std;
using namespace kuzu::common;

namespace kuzu {
namespace function {
namespace operation {

struct Rpad : BasePadOperation {
public:
    static inline void operation(ku_string_t& src, int64_t count, ku_string_t& characterToPad,
        ku_string_t& result, ValueVector& resultValueVector) {
        BasePadOperation::operation(
            src, count, characterToPad, result, resultValueVector, rpadOperation);
    }

    static void rpadOperation(
        ku_string_t& src, int64_t count, ku_string_t& characterToPad, string& paddedResult) {
        auto srcPadInfo =
            BasePadOperation::padCountChars(count, (const char*)src.getData(), src.len);
        auto srcData = (const char*)src.getData();
        paddedResult.insert(paddedResult.end(), srcData, srcData + srcPadInfo.first);
        BasePadOperation::insertPadding(count - srcPadInfo.second, characterToPad, paddedResult);
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
