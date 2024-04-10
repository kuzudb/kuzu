#pragma once

#include "function/function.h"
#include "function/table/call_functions.h"

namespace kuzu {
namespace graph {

// CallFunction has the assumption that number of output is known before execution.
// This does NOT hold for all graph algorithms.
// So each algorithm need to decide its own shared state to control when to terminate.
struct DemoAlgorithm  {
    static constexpr const char* name = "DEMO_ALGO";

    static function::function_set getFunctionSet();
};

struct VariableLengthPath  {
    static constexpr const char* name = "VARIABLE_LENGTH_PATH";

    static function::function_set getFunctionSet();
};

struct ShortestPath {
    static constexpr const char* name = "DEMO_ALGO";

    static function::function_set getFunctionSet();
};

struct PageRank {
    static constexpr const char* name = "DEMO_ALGO";

    static function::function_set getFunctionSet();
};

} // namespace graph
} // namespace kuzu
