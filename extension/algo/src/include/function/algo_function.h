#pragma once

#include "function/function.h"

namespace kuzu {
namespace algo_extension {

struct SCCFunction {
    static constexpr const char* name = "STRONGLY_CONNECTED_COMPONENTS";

    static function::function_set getFunctionSet();
};

struct SCCAliasFunction {
    using alias = SCCFunction;

    static constexpr const char* name = "SCC";
};

struct SCCKosarajuFunction {
    static constexpr const char* name = "STRONGLY_CONNECTED_COMPONENTS_KOSARAJU";

    static function::function_set getFunctionSet();
};

struct SCCKosarajuAliasFunction {
    using alias = SCCKosarajuFunction;

    static constexpr const char* name = "SCC_KO";
};

struct WeaklyConnectedComponentsFunction {
    static constexpr const char* name = "WEAKLY_CONNECTED_COMPONENTS";

    static function::function_set getFunctionSet();
};

struct WeaklyConnectedComponentsAliasFunction {
    using alias = WeaklyConnectedComponentsFunction;

    static constexpr const char* name = "WCC";
};

struct PageRankFunction {
    static constexpr const char* name = "PAGE_RANK";

    static function::function_set getFunctionSet();
};

struct PageRankAliasFunction {
    using alias = PageRankFunction;

    static constexpr const char* name = "PR";
};

struct KCoreDecompositionFunction {
    static constexpr const char* name = "K_CORE_DECOMPOSITION";

    static function::function_set getFunctionSet();
};

struct KCoreDecompositionAliasFunction {
    using alias = KCoreDecompositionFunction;

    static constexpr const char* name = "KCORE";
};

struct LouvainFunction {
    static constexpr const char* name = "LOUVAIN";

    static function::function_set getFunctionSet();
};

struct SpanningForest {
    static constexpr const char* name = "SPANNING_FOREST";

    static function::function_set getFunctionSet();
};

struct SpanningForestAliasFunction {
    using alias = SpanningForest;

    static constexpr const char* name = "SF";
};

} // namespace algo_extension
} // namespace kuzu
