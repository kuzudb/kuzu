#pragma once

#include "function/vector_functions.h"

namespace kuzu {
namespace function {
class VectorTimestampFunction : public VectorFunction {};

struct CenturyVectorFunction : public VectorTimestampFunction {
    static vector_function_definitions getDefinitions();
};

struct EpochMsVectorFunction : public VectorTimestampFunction {
    static vector_function_definitions getDefinitions();
};

struct ToTimestampVectorFunction : public VectorTimestampFunction {
    static vector_function_definitions getDefinitions();
};

} // namespace function
} // namespace kuzu
