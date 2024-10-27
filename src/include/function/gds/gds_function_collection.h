#pragma once

#include "function/gds_function.h"

namespace kuzu {
namespace function {

struct WeaklyConnectedComponentsFunction {
    static constexpr const char* name = "WEAKLY_CONNECTED_COMPONENT";

    static function_set getFunctionSet();
};

struct VarLenJoinsFunction {
    static constexpr const char* name = "VAR_LEN_JOINS";

    static function_set getFunctionSet();
    static GDSFunction getFunction();
};

struct AllSPDestinationsFunction {
    static constexpr const char* name = "ALL_SP_DESTINATIONS";

    static function_set getFunctionSet();
    static GDSFunction getFunction();
};

struct AllSPPathsFunction {
    static constexpr const char* name = "ALL_SP_PATHS";

    static function_set getFunctionSet();
    static GDSFunction getFunction();
};

struct SingleSPDestinationsFunction {
    static constexpr const char* name = "SINGLE_SP_DESTINATIONS";

    static function_set getFunctionSet();
    static GDSFunction getFunction();
};

struct SingleSPPathsFunction {
    static constexpr const char* name = "SINGLE_SP_PATHS";

    static function_set getFunctionSet();
    static GDSFunction getFunction();
};

struct PageRankFunction {
    static constexpr const char* name = "PAGE_RANK";

    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
