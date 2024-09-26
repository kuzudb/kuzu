#pragma once

#include "function/function.h"

namespace kuzu {
namespace fts_extension {

struct StemFunction {
    static constexpr const char* name = "stem";

    static function::function_set getFunctionSet();
};

} // namespace fts_extension
} // namespace kuzu
