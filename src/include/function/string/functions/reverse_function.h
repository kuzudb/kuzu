#pragma once

#include <cassert>
#include <cstring>

#include "base_str_function.h"
#include "common/api.h"
#include "common/types/ku_string.h"

namespace kuzu {
namespace function {

struct Reverse {
public:
    KUZU_API static void operation(common::ku_string_t& input, common::ku_string_t& result,
        common::ValueVector& resultValueVector);

private:
    static uint32_t reverseStr(char* data, uint32_t len) {
        for (auto i = 0u; i < len / 2; i++) {
            std::swap(data[i], data[len - i - 1]);
        }
        return len;
    }
};

} // namespace function
} // namespace kuzu
