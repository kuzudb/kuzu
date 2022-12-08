#pragma once

#include <cassert>
#include <cstring>

#include "base_pad_operation.h"
#include "common/types/ku_string.h"

using namespace std;
using namespace kuzu::function::operation;

namespace kuzu {
namespace function {
namespace operation {

struct Lpad : BasePadOperation {
public:
    static inline void operation(ku_string_t& src, int64_t count, ku_string_t& characterToPad,
        ku_string_t& result, ValueVector& resultValueVector) {
        BasePadOperation::operation(
            src, count, characterToPad, result, resultValueVector, lpadOperation);
    }

    static void lpadOperation(
        ku_string_t& src, int64_t count, ku_string_t& characterToPad, string& paddedResult) {
        auto srcPadInfo =
            BasePadOperation::padCountChars(count, (const char*)src.getData(), src.len);
        auto srcData = (const char*)src.getData();
        BasePadOperation::insertPadding(count - srcPadInfo.second, characterToPad, paddedResult);
        paddedResult.insert(paddedResult.end(), srcData, srcData + srcPadInfo.first);
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
