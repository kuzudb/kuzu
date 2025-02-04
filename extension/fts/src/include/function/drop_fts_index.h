#pragma once

#include "function/function.h"

namespace kuzu {
namespace fts_extension {

struct DropFTSFunction {
    static constexpr const char* name = "DROP_FTS_INDEX";

    static function::function_set getFunctionSet();
};

} // namespace fts_extension
} // namespace kuzu
