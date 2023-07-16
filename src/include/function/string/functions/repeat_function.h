#pragma once

#include <cassert>
#include <cstring>

#include "common/types/ku_string.h"

namespace kuzu {
namespace function {

struct Repeat {
public:
    static inline void operation(common::ku_string_t& left, int64_t& right,
        common::ku_string_t& result, common::ValueVector& resultValueVector) {
        result.len = left.len * right;
        if (result.len <= common::ku_string_t::SHORT_STR_LENGTH) {
            repeatStr((char*)result.prefix, left.getAsString(), right);
        } else {
            result.overflowPtr = reinterpret_cast<uint64_t>(
                common::StringVector::getInMemOverflowBuffer(&resultValueVector)
                    ->allocateSpace(result.len));
            auto buffer = reinterpret_cast<char*>(result.overflowPtr);
            repeatStr(buffer, left.getAsString(), right);
            memcpy(result.prefix, buffer, common::ku_string_t::PREFIX_LENGTH);
        }
    }

private:
    static void repeatStr(char* data, std::string pattern, uint64_t count) {
        for (auto i = 0u; i < count; i++) {
            memcpy(data + i * pattern.length(), pattern.c_str(), pattern.length());
        }
    }
};

} // namespace function
} // namespace kuzu
