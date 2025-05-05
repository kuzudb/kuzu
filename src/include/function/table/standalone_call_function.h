#pragma once

#include "function/function.h"

namespace kuzu {
namespace function {

struct ClearWarningsFunction final {
    static constexpr const char* name = "CLEAR_WARNINGS";

    static function_set getFunctionSet();
};

struct ProjectGraphFunction final {
    static constexpr const char* name = "PROJECT_GRAPH";

    static function_set getFunctionSet();
};

struct DropProjectedGraphFunction final {
    static constexpr const char* name = "DROP_PROJECTED_GRAPH";

    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
