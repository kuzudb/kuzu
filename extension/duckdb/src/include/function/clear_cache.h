#pragma once

#include "common/types/types.h"
#include "function/table/simple_table_functions.h"
#include "function/table_functions.h"
#include "main/database_manager.h"

namespace kuzu {
namespace duckdb_extension {

struct ClearCacheBindData final : function::SimpleTableFuncBindData {
    main::DatabaseManager* databaseManager;

    ClearCacheBindData(main::DatabaseManager* databaseManager, binder::expression_vector columns,
        common::offset_t maxOffset)
        : SimpleTableFuncBindData{std::move(columns), maxOffset}, databaseManager{databaseManager} {
    }

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<ClearCacheBindData>(databaseManager, columns, maxOffset);
    }
};

struct ClearCacheFunction final : function::TableFunction {
    static constexpr const char* name = "clear_attached_db_cache";

    ClearCacheFunction();
};

} // namespace duckdb_extension
} // namespace kuzu
