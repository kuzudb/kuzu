#pragma once

#include <cassert>
#include <cstring>

#include "src/common/types/include/ku_string.h"

using namespace std;
using namespace kuzu::common;

namespace kuzu {
namespace function {
namespace operation {

struct SubStr {
public:
    static inline void operation(ku_string_t& src, int64_t start, int64_t len, ku_string_t& result,
        ValueVector& resultValueVector) {
        result.len = min(len, src.len - start + 1);
        if (!ku_string_t::isShortString(result.len)) {
            result.overflowPtr = reinterpret_cast<uint64_t>(
                resultValueVector.getOverflowBuffer().allocateSpace(result.len));
        }
        memcpy((uint8_t*)result.getData(), src.getData() + start - 1, result.len);
        if (!ku_string_t::isShortString(result.len)) {
            memcpy(result.prefix, result.getData(), ku_string_t::PREFIX_LENGTH);
        }
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
