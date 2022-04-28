#pragma once

#include <cassert>
#include <cstring>

#include "src/common/types/include/gf_string.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace function {
namespace operation {

struct SubStr {
public:
    static inline void operation(gf_string_t& src, int64_t start, int64_t len, gf_string_t& result,
        bool isSrcNull, bool isStartNull, bool isLenNull, ValueVector& resultValueVector) {
        assert(!isSrcNull && !isStartNull && !isLenNull);
        result.len = min(len, src.len - start + 1);
        if (!gf_string_t::isShortString(result.len)) {
            result.overflowPtr = reinterpret_cast<uint64_t>(
                resultValueVector.getOverflowBuffer().allocateSpace(result.len));
        }
        memcpy((uint8_t*)result.getData(), src.getData() + start - 1, len);
        if (!gf_string_t::isShortString(result.len)) {
            memcpy(result.prefix, result.getData(), gf_string_t::PREFIX_LENGTH);
        }
    }
};

} // namespace operation
} // namespace function
} // namespace graphflow
