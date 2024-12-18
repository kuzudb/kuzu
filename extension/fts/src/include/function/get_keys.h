#pragma once

#include "function/function.h"

namespace kuzu {
namespace fts_extension {

struct GetKeysFunction {
    static constexpr const char* name = "GET_KEYS";

    static function::function_set getFunctionSet();
};

} // namespace fts_extension
} // namespace kuzu
