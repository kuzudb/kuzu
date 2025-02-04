#pragma once

#include "function/function.h"

namespace kuzu {
namespace fts_extension {

struct CreateFTSFunction {
    static constexpr const char* name = "CREATE_FTS_INDEX";
    static constexpr const char* DOC_LEN_PROP_NAME = "LEN";

    static function::function_set getFunctionSet();
};

} // namespace fts_extension
} // namespace kuzu
