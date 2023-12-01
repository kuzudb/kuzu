#pragma once

#include "function/scalar_function.h"

namespace kuzu {
namespace function {

struct MapCreationFunctions {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* function);
};

struct MapExtractFunctions {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* function);
};

struct MapKeysFunctions {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* function);
};

struct MapValuesFunctions {
    static function_set getFunctionSet();
    static std::unique_ptr<FunctionBindData> bindFunc(
        const binder::expression_vector& arguments, Function* function);
};

} // namespace function
} // namespace kuzu
