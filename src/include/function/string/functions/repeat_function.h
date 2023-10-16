#pragma once

#include <cassert>
#include <cstring>

#include "common/api.h"
#include "common/types/ku_string.h"

namespace kuzu {
namespace function {

struct Repeat {
public:
    KUZU_API static void operation(common::ku_string_t& left, int64_t& right,
        common::ku_string_t& result, common::ValueVector& resultValueVector);

private:
    static void repeatStr(char* data, std::string pattern, uint64_t count) {
        for (auto i = 0u; i < count; i++) {
            memcpy(data + i * pattern.length(), pattern.c_str(), pattern.length());
        }
    }
};

} // namespace function
} // namespace kuzu
