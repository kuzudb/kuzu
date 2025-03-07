#pragma once

#include "common/types/types.h"
#include "function/table/bind_data.h"
#include "function/table/table_function.h"

namespace kuzu {
namespace postgres_extension {

struct SqlQueryFunction final {
    static constexpr const char* name = "sql_query";

    static function::function_set getFunctionSet();
};

} // namespace postgres_extension
} // namespace kuzu
