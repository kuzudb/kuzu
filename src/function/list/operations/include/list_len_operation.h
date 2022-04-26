#pragma once

#include <cassert>
#include <cstring>

#include "src/common/types/include/gf_list.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace function {
namespace operation {

struct ListLen {
public:
    static inline void operation(gf_list_t& input, bool isNull, int64_t& result) {
        assert(!isNull);
        result = input.size;
    }
};

} // namespace operation
} // namespace function
} // namespace graphflow
