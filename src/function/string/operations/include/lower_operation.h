#pragma once

#include <cassert>
#include <cstring>

#include "src/common/types/include/gf_string.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace function {
namespace operation {

struct Lower {
public:
    static inline void operation(
        gf_string_t& input, bool isNull, gf_string_t& result, ValueVector& resultValueVector) {
        assert(!isNull);
        auto len = input.len;
        if (len <= gf_string_t::SHORT_STR_LENGTH) {
            memcpy(result.prefix, input.prefix, len);
            lowerStr((char*)result.prefix, len);
        } else {
            result.overflowPtr = reinterpret_cast<uint64_t>(
                resultValueVector.getOverflowBuffer().allocateSpace(len));
            auto buffer = reinterpret_cast<char*>(result.overflowPtr);
            memcpy(buffer, input.getData(), len);
            lowerStr(buffer, len);
            memcpy(result.prefix, buffer, gf_string_t::PREFIX_LENGTH);
        }
        result.len = len;
    }

private:
    static void lowerStr(char* str, uint32_t len) {
        for (auto i = 0u; i < len; i++) {
            str[i] = tolower(str[i]);
        }
    }
};

} // namespace operation
} // namespace function
} // namespace graphflow
