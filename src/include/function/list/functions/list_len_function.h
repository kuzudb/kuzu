#pragma once

#include <cassert>
#include <cstring>

#include "common/types/ku_list.h"

namespace kuzu {
namespace function {

struct ListLen {
public:
    static inline void operation(common::list_entry_t& input, int64_t& result) {
        result = input.size;
    }
};

} // namespace function
} // namespace kuzu
