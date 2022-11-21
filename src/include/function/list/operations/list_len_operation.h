#pragma once

#include <cassert>
#include <cstring>

#include "common/types/ku_list.h"

using namespace std;
using namespace kuzu::common;

namespace kuzu {
namespace function {
namespace operation {

struct ListLen {
public:
    static inline void operation(ku_list_t& input, int64_t& result) { result = input.size; }
};

} // namespace operation
} // namespace function
} // namespace kuzu
