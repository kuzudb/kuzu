#pragma once

#include <cassert>
#include <cstring>

#include "base_pad_operation.h"

#include "src/common/types/include/gf_string.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace function {
namespace operation {

struct Lpad : BasePadOperation {
public:
    static inline void operation(gf_string_t& src, int64_t count, gf_string_t& characterToPad,
        gf_string_t& result, ValueVector& resultValueVector) {
        BasePadOperation::operation(
            src, count, characterToPad, result, resultValueVector, lpadOperation);
    }

    static void lpadOperation(
        gf_string_t& src, int64_t count, gf_string_t& characterToPad, string& paddedResult) {
        auto srcPadInfo =
            BasePadOperation::padCountChars(count, (const char*)src.getData(), src.len);
        auto srcData = (const char*)src.getData();
        BasePadOperation::insertPadding(count - srcPadInfo.second, characterToPad, paddedResult);
        paddedResult.insert(paddedResult.end(), srcData, srcData + srcPadInfo.first);
    }
};

} // namespace operation
} // namespace function
} // namespace graphflow
