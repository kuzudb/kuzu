#pragma once

#include "function/vector_functions.h"

namespace kuzu {
namespace function {

struct OctetLengthVectorFunctions : public VectorFunction {
    static vector_function_definitions getDefinitions();
};

struct EncodeVectorFunctions : public VectorFunction {
    static vector_function_definitions getDefinitions();
};

struct DecodeVectorFunctions : public VectorFunction {
    static vector_function_definitions getDefinitions();
};

} // namespace function
} // namespace kuzu
