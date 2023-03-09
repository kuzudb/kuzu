#pragma once

#include <cassert>
#include <cstring>

#include "common/types/ku_string.h"

namespace kuzu {
namespace function {
namespace operation {

struct PadOperation {
public:
    static inline void operation(common::ku_string_t& src, int64_t count,
        common::ku_string_t& characterToPad, common::ku_string_t& result,
        common::ValueVector& resultValueVector,
        void (*padOperation)(common::ku_string_t& result, common::ku_string_t& src,
            common::ku_string_t& characterToPad)) {
        if (count <= 0) {
            result.set("", 0);
            return;
        }
        assert(characterToPad.len == 1);
        result.len = count;
        if (!common::ku_string_t::isShortString(result.len)) {
            result.overflowPtr = reinterpret_cast<uint64_t>(
                resultValueVector.getOverflowBuffer().allocateSpace(result.len));
        }
        padOperation(result, src, characterToPad);
        if (!common::ku_string_t::isShortString(result.len)) {
            memcpy(result.prefix, result.getData(), common::ku_string_t::PREFIX_LENGTH);
        }
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
