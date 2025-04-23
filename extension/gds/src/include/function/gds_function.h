#pragma once

#include "function/function.h"

namespace kuzu {
namespace gds_extension {

struct SCCFunction {
    static constexpr const char* name = "STRONGLY_CONNECTED_COMPONENTS";

    static function::function_set getFunctionSet();
};

struct SCCKosarajuFunction {
    static constexpr const char* name = "STRONGLY_CONNECTED_COMPONENTS_KOSARAJU";

    static function::function_set getFunctionSet();
};

struct WeaklyConnectedComponentsFunction {
    static constexpr const char* name = "WEAKLY_CONNECTED_COMPONENT";

    static function::function_set getFunctionSet();
};

struct PageRankFunction {
    static constexpr const char* name = "PAGE_RANK";

    static function::function_set getFunctionSet();
};

struct KCoreDecompositionFunction {
    static constexpr const char* name = "K_CORE_DECOMPOSITION";

    static function_set getFunctionSet();
};

} // namespace gds_extension
} // namespace kuzu
