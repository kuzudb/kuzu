#pragma once

#include <cassert>
#include <cstring>

#include "common/types/ku_string.h"

using namespace std;
using namespace kuzu::common;

namespace kuzu {
namespace function {
namespace operation {

struct PadOperation {
public:
    static inline void operation(ku_string_t& src, int64_t count, ku_string_t& characterToPad,
        ku_string_t& result, ValueVector& resultValueVector,
        void (*padOperation)(ku_string_t& result, ku_string_t& src, ku_string_t& characterToPad)) {
        if (count <= 0) {
            result.set("", 0);
            return;
        }
        assert(characterToPad.len == 1);
        result.len = count;
        if (!ku_string_t::isShortString(result.len)) {
            result.overflowPtr = reinterpret_cast<uint64_t>(
                resultValueVector.getOverflowBuffer().allocateSpace(result.len));
        }
        padOperation(result, src, characterToPad);
        if (!ku_string_t::isShortString(result.len)) {
            memcpy(result.prefix, result.getData(), ku_string_t::PREFIX_LENGTH);
        }
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
