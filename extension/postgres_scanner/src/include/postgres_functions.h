#pragma once

#include "function/table_functions.h"

namespace kuzu {
namespace postgres_scanner {

struct PostgresClearCacheFunction final : public function::TableFunction {
    static constexpr const char* name = "postgres_clear_cache";

    PostgresClearCacheFunction();
};

} // namespace postgres_scanner
} // namespace kuzu
