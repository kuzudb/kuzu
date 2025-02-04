#pragma once

#include "common/types/types.h"
#include "function/table/bind_data.h"
#include "function/table/table_function.h"

namespace kuzu {
namespace duckdb_extension {

struct ClearCacheBindData final : function::TableFuncBindData {
    main::DatabaseManager* databaseManager;

    explicit ClearCacheBindData(main::DatabaseManager* databaseManager)
        : TableFuncBindData{binder::expression_vector{}, 1 /* maxOffset */},
          databaseManager{databaseManager} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<ClearCacheBindData>(databaseManager);
    }
};

struct ClearCacheFunction final {
    static constexpr const char* name = "clear_attached_db_cache";

    static function::function_set getFunctionSet();
};

} // namespace duckdb_extension
} // namespace kuzu
