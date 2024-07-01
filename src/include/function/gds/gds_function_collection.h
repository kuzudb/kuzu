#pragma once

#include "function/function.h"

namespace kuzu {
namespace function {

struct WeaklyConnectedComponentsFunction {
    static constexpr const char* name = "WEAKLY_CONNECTED_COMPONENT";

    static function_set getFunctionSet();
};

struct ShortestPathsFunction {
    static constexpr const char* name = "SHORTEST_PATHS";

    static function_set getFunctionSet();
};

struct PageRankFunction {
    static constexpr const char* name = "PAGE_RANK";

    static function_set getFunctionSet();
};

struct _1T1SParallelShortestPathFunction {
    static constexpr const char* name = "_1T1S_PARALLEL_SHORTEST_PATH";

    static function_set getFunctionSet();
};

struct nT1SParallelShortestPathsFunction {
    static constexpr const char* name = "nT1S_PARALLEL_SHORTEST_PATH";

    static function_set getFunctionSet();
};

struct nTkSParallelShortestPathsFunction {
    static constexpr const char* name = "nTkS_PARALLEL_SHORTEST_PATH";

    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
