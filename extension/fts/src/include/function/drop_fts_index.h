#pragma once

#include "function/table/simple_table_functions.h"

namespace kuzu {
namespace fts_extension {

struct DropFTSFunction : function::SimpleTableFunction {
    static constexpr const char* name = "DROP_FTS_INDEX";

    static function::function_set getFunctionSet();
};

} // namespace fts_extension
} // namespace kuzu
