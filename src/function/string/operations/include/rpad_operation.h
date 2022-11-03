#pragma once

#include <cassert>
#include <cstring>

#include "pad_operation.h"

#include "src/common/types/include/gf_string.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace function {
namespace operation {

struct Rpad : PadOperation {
public:
    static inline void operation(gf_string_t& src, int64_t count, gf_string_t& characterToPad,
        gf_string_t& result, ValueVector& resultValueVector) {
        PadOperation::operation(
            src, count, characterToPad, result, resultValueVector, rpadOperation);
    }

    static void rpadOperation(
        gf_string_t& src, int64_t count, gf_string_t& characterToPad, vector<char>& paddedResult) {
        auto srcPadInfo = PadOperation::padCountChars(count, (const char*)src.getData(), src.len);
        auto srcData = (const char*)src.getData();
        paddedResult.insert(paddedResult.end(), srcData, srcData + srcPadInfo.first);
        PadOperation::insertPadding(count - srcPadInfo.second, characterToPad, paddedResult);
    }
};

} // namespace operation
} // namespace function
} // namespace graphflow
