#pragma once

#include "function/function.h"
#include "function/gds/rec_joins.h"

namespace kuzu {
namespace function {

struct WeaklyConnectedComponentsFunction {
    static constexpr const char* name = "WEAKLY_CONNECTED_COMPONENT";

    static function_set getFunctionSet();
};

struct SCCKosarajuFunction {
    static constexpr const char* name = "STRONGLY_CONNECTED_COMPONENTS_KOSARAJU";

    static function_set getFunctionSet();
};

struct SCCFunction {
    static constexpr const char* name = "STRONGLY_CONNECTED_COMPONENTS";

    static function_set getFunctionSet();
};

struct KCoreDecompositionFunction {
    static constexpr const char* name = "K_CORE_DECOMPOSITION";

    static function_set getFunctionSet();
};

struct PageRankFunction {
    static constexpr const char* name = "PAGE_RANK";

    static function_set getFunctionSet();
};

struct VarLenJoinsFunction {
    static constexpr const char* name = "VAR_LEN_JOINS";

    static std::unique_ptr<RJAlgorithm> getAlgorithm();
};

struct AllSPDestinationsFunction {
    static constexpr const char* name = "ALL_SP_DESTINATIONS";

    static std::unique_ptr<RJAlgorithm> getAlgorithm();
};

struct AllSPPathsFunction {
    static constexpr const char* name = "ALL_SP_PATHS";

    static std::unique_ptr<RJAlgorithm> getAlgorithm();
};

struct SingleSPDestinationsFunction {
    static constexpr const char* name = "SINGLE_SP_DESTINATIONS";

    static std::unique_ptr<RJAlgorithm> getAlgorithm();
};

struct SingleSPPathsFunction {
    static constexpr const char* name = "SINGLE_SP_PATHS";

    static std::unique_ptr<RJAlgorithm> getAlgorithm();
};

struct WeightedSPDestinationsFunction {
    static constexpr const char* name = "WEIGHTED_SP_DESTINATIONS";

    static std::unique_ptr<RJAlgorithm> getAlgorithm();
};

struct WeightedSPPathsFunction {
    static constexpr const char* name = "WEIGHTED_SP_PATHS";

    static std::unique_ptr<RJAlgorithm> getAlgorithm();
};

struct AllWeightedSPPathsFunction {
    static constexpr const char* name = "ALL_WEIGHTED_SP_PATHS";

    static std::unique_ptr<RJAlgorithm> getAlgorithm();
};

} // namespace function
} // namespace kuzu
