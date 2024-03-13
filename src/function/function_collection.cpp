#include "function/function_collection.h"

#include "function/arithmetic/vector_arithmetic_functions.h"

namespace kuzu {
namespace function {

#define SCALAR_FUNCTION(_PARAM)                                                                    \
    { _PARAM::name, _PARAM::getFunctionSet }
#define FINAL_FUNCTION                                                                             \
    { nullptr, nullptr }

static FunctionCollection functions[] = {SCALAR_FUNCTION(AbsFunction), FINAL_FUNCTION};

FunctionCollection* FunctionCollection::getFunctions() {
    return functions;
}

} // namespace function
} // namespace kuzu
