#pragma once

#include "function/function.h"

namespace kuzu {
namespace function {

struct VariableLengthPathsFunction {
    static constexpr const char* name = "VARIABLE_LENGTH_PATH";

    static function_set getFunctionSet();
};

struct WeaklyConnectedComponentsFunction {
    static constexpr const char* name = "WEAKLY_CONNECTED_COMPONENT";

    static function_set getFunctionSet();
};

struct ShortestPathsFunction {
    static constexpr const char* name = "SHORTEST_PATH";

    static function_set getFunctionSet();
};

struct PageRankFunction {
    static constexpr const char* name = "PAGE_RANK";

    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
