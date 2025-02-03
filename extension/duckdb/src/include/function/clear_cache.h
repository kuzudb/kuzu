#pragma once

#include "common/types/types.h"
#include "function/table/simple_table_functions.h"
#include "function/table_functions.h"

namespace kuzu {
namespace duckdb_extension {

struct ClearCacheBindData final : function::SimpleTableFuncBindData {
    main::DatabaseManager* databaseManager;

    explicit ClearCacheBindData(main::DatabaseManager* databaseManager)
        : SimpleTableFuncBindData{binder::expression_vector{}, 1 /* maxOffset */},
          databaseManager{databaseManager} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<ClearCacheBindData>(databaseManager);
    }
};

struct ClearCacheFunction final : function::TableFunction {
    static constexpr const char* name = "clear_attached_db_cache";
    static function::function_set getFunctionSet();
};

} // namespace duckdb_extension
} // namespace kuzu
