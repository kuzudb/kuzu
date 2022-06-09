#pragma once

#include <cassert>
#include <cstring>

#include "src/common/types/include/gf_string.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace function {
namespace operation {

struct PadOperation {
public:
    static inline void operation(gf_string_t& src, int64_t count, gf_string_t& characterToPad,
        gf_string_t& result, ValueVector& resultValueVector,
        void (*padOperation)(gf_string_t& result, gf_string_t& src, gf_string_t& characterToPad)) {
        assert(characterToPad.len == 1);
        result.len = count;
        if (!gf_string_t::isShortString(result.len)) {
            result.overflowPtr = reinterpret_cast<uint64_t>(
                resultValueVector.getOverflowBuffer().allocateSpace(result.len));
        }
        padOperation(result, src, characterToPad);
        if (!gf_string_t::isShortString(result.len)) {
            memcpy(result.prefix, result.getData(), gf_string_t::PREFIX_LENGTH);
        }
    }
};

} // namespace operation
} // namespace function
} // namespace graphflow
