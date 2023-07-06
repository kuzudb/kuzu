#pragma once

#include "function/vector_functions.h"

namespace kuzu {
namespace function {
class VectorDateFunction : public VectorFunction {};

struct DatePartVectorFunction : public VectorDateFunction {
    static vector_function_definitions getDefinitions();
};

struct DateTruncVectorFunction : public VectorDateFunction {
    static vector_function_definitions getDefinitions();
};

struct DayNameVectorFunction : public VectorDateFunction {
    static vector_function_definitions getDefinitions();
};

struct GreatestVectorFunction : public VectorDateFunction {
    static vector_function_definitions getDefinitions();
};

struct LastDayVectorFunction : public VectorDateFunction {
    static vector_function_definitions getDefinitions();
};

struct LeastVectorFunction : public VectorDateFunction {
    static vector_function_definitions getDefinitions();
};

struct MakeDateVectorFunction : public VectorDateFunction {
    static vector_function_definitions getDefinitions();
};

struct MonthNameVectorFunction : public VectorDateFunction {
    static vector_function_definitions getDefinitions();
};

} // namespace function
} // namespace kuzu
