#pragma once

#include <cassert>
#include <cstring>

#include "common/types/ku_list.h"

namespace kuzu {
namespace function {
namespace operation {

struct ListLen {
public:
    static inline void operation(common::ku_list_t& input, int64_t& result) { result = input.size; }
};

} // namespace operation
} // namespace function
} // namespace kuzu
