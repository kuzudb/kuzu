#pragma once

#include <cassert>
#include <cstring>

#include "common/types/ku_string.h"

using namespace std;
using namespace kuzu::common;

namespace kuzu {
namespace function {
namespace operation {

struct BaseStrOperation {
public:
    static inline void operation(ku_string_t& input, ku_string_t& result,
        ValueVector& resultValueVector, uint32_t (*strOperation)(char* data, uint32_t len)) {
        if (input.len <= ku_string_t::SHORT_STR_LENGTH) {
            memcpy(result.prefix, input.prefix, input.len);
            result.len = strOperation((char*)result.prefix, input.len);
        } else {
            result.overflowPtr = reinterpret_cast<uint64_t>(
                resultValueVector.getOverflowBuffer().allocateSpace(input.len));
            auto buffer = reinterpret_cast<char*>(result.overflowPtr);
            memcpy(buffer, input.getData(), input.len);
            result.len = strOperation(buffer, input.len);
            memcpy(result.prefix, buffer,
                result.len < ku_string_t::PREFIX_LENGTH ? result.len : ku_string_t::PREFIX_LENGTH);
        }
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
