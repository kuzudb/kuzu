#pragma once

#include <cassert>
#include <cstring>

#include "common/types/ku_string.h"

namespace kuzu {
namespace function {

struct BaseStrOperation {
public:
    static inline void operation(common::ku_string_t& input, common::ku_string_t& result,
        common::ValueVector& resultValueVector,
        uint32_t (*strOperation)(char* data, uint32_t len)) {
        if (input.len <= common::ku_string_t::SHORT_STR_LENGTH) {
            memcpy(result.prefix, input.prefix, input.len);
            result.len = strOperation((char*)result.prefix, input.len);
        } else {
            result.overflowPtr = reinterpret_cast<uint64_t>(
                common::StringVector::getInMemOverflowBuffer(&resultValueVector)
                    ->allocateSpace(input.len));
            auto buffer = reinterpret_cast<char*>(result.overflowPtr);
            memcpy(buffer, input.getData(), input.len);
            result.len = strOperation(buffer, input.len);
            memcpy(result.prefix, buffer,
                result.len < common::ku_string_t::PREFIX_LENGTH ?
                    result.len :
                    common::ku_string_t::PREFIX_LENGTH);
        }
    }
};

} // namespace function
} // namespace kuzu
