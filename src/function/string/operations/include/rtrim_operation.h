#pragma once

#include <cassert>
#include <cstring>

#include "src/common/types/include/gf_string.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace function {
namespace operation {

struct Rtrim {
public:
    static inline void operation(
        gf_string_t& input, bool isNull, gf_string_t& result, ValueVector& resultValueVector) {
        assert(!isNull);
        if (input.len <= gf_string_t::SHORT_STR_LENGTH) {
            memcpy(result.prefix, input.prefix, input.len);
            result.len = rtrim((char*)result.prefix, input.len);
        } else {
            result.overflowPtr = reinterpret_cast<uint64_t>(
                resultValueVector.getOverflowBuffer().allocateSpace(input.len));
            auto buffer = reinterpret_cast<char*>(result.overflowPtr);
            memcpy(buffer, input.getData(), input.len);
            result.len = rtrim(buffer, input.len);
            memcpy(result.prefix, buffer,
                result.len < gf_string_t::PREFIX_LENGTH ? result.len : gf_string_t::PREFIX_LENGTH);
        }
    }

private:
    static uint32_t rtrim(char* data, uint32_t len) {
        auto counter = len - 1;
        for (; counter >= 0; counter--) {
            if (!isspace(data[counter])) {
                break;
            }
        }
        return counter + 1;
    }
};

} // namespace operation
} // namespace function
} // namespace graphflow
