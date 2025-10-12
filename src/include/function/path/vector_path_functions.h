#pragma once

#include "function/function.h"

namespace kuzu {
namespace function {

struct NodesFunction {
    static constexpr const char* name = "NODES";

    static function_set getFunctionSet();
};

struct RelsFunction {
    static constexpr const char* name = "RELS";

    static function_set getFunctionSet();
};

struct RelationshipsFunction {
    using alias = RelsFunction;

    static constexpr const char* name = "RELATIONSHIPS";
};

struct IsTrailFunction {
    static constexpr const char* name = "IS_TRAIL";

    static function_set getFunctionSet();
};

struct IsACyclicFunction {
    static constexpr const char* name = "IS_ACYCLIC";

    static function_set getFunctionSet();
};

struct LengthFunction {
    static constexpr const char* name = "LENGTH";

    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
