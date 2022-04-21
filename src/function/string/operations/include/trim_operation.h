#pragma once

#include <cassert>
#include <cstring>

#include "ltrim_operation.h"
#include "rtrim_operation.h"

#include "src/common/types/include/gf_string.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace function {
namespace operation {

struct Trim : BaseStrOperation {
public:
    static inline void operation(
        gf_string_t& input, bool isNull, gf_string_t& result, ValueVector& resultValueVector) {
        BaseStrOperation::operation(input, isNull, result, resultValueVector, trim);
    }

private:
    static uint32_t trim(char* data, uint32_t len) {
        return Rtrim::rtrim(data, Ltrim::ltrim(data, len));
    }
};

} // namespace operation
} // namespace function
} // namespace graphflow
