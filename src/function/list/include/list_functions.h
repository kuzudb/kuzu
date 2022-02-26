#pragma once

#include <functional>

#include "src/common/include/vector/value_vector.h"

using namespace graphflow::common;

namespace graphflow {
namespace function {

using list_func = std::function<void(const vector<shared_ptr<ValueVector>>&, ValueVector&)>;

struct ListFunctions {

    static void ListCreation(
        const vector<shared_ptr<ValueVector>>& parameters, ValueVector& result);
};

} // namespace function
} // namespace graphflow
