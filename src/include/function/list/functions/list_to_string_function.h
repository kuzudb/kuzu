#pragma once

#include "common/types/ku_string.h"
#include "common/vector/value_vector.h"

namespace kuzu {
namespace function {

struct ListToString {
    static void operation(common::list_entry_t& input, common::ku_string_t& delim,
        common::ku_string_t& result, common::ValueVector& inputVector,
        common::ValueVector& /*delimVector*/, common::ValueVector& resultVector);
};

} // namespace function
} // namespace kuzu
