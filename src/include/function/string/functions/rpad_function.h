#pragma once

#include "base_pad_function.h"
#include "common/types/ku_string.h"

namespace kuzu {
namespace function {

struct Rpad : BasePadOperation {
    static void operation(common::ku_string_t& src, int64_t count,
        common::ku_string_t& characterToPad, common::ku_string_t& result,
        common::ValueVector& resultValueVector) {
        BasePadOperation::operation(src, count, characterToPad, result, resultValueVector,
            rpadOperation);
    }

    static void rpadOperation(common::ku_string_t& src, int64_t count,
        common::ku_string_t& characterToPad, std::string& paddedResult) {
        auto srcPadInfo =
            padCountChars(count, reinterpret_cast<const char*>(src.getData()), src.len);
        auto srcData = reinterpret_cast<const char*>(src.getData());
        paddedResult.insert(paddedResult.end(), srcData, srcData + srcPadInfo.first);
        insertPadding(count - srcPadInfo.second, characterToPad, paddedResult);
    }
};

} // namespace function
} // namespace kuzu
