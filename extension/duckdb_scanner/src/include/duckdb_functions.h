#pragma once

#include "common/types/types.h"
#include "function/table/call_functions.h"
#include "function/table_functions.h"
#include "main/database_manager.h"

namespace kuzu {
namespace duckdb_scanner {

struct DuckDBClearCacheBindData : public function::CallTableFuncBindData {
    main::DatabaseManager* databaseManager;

    DuckDBClearCacheBindData(main::DatabaseManager* databaseManager,
        std::vector<common::LogicalType> returnTypes, std::vector<std::string> returnColumnNames,
        common::offset_t maxOffset)
        : CallTableFuncBindData{std::move(returnTypes), std::move(returnColumnNames), maxOffset},
          databaseManager{databaseManager} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<DuckDBClearCacheBindData>(databaseManager, columnTypes, columnNames,
            maxOffset);
    }
};

struct DuckDBClearCacheFunction final : public function::TableFunction {
    static constexpr const char* name = "duckdb_clear_cache";

    DuckDBClearCacheFunction();
};

} // namespace duckdb_scanner
} // namespace kuzu
