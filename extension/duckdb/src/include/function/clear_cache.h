#pragma once

#include "common/types/types.h"
#include "function/table/call_functions.h"
#include "function/table_functions.h"
#include "main/database_manager.h"

namespace kuzu {
namespace duckdb_extension {

struct ClearCacheBindData : public function::CallTableFuncBindData {
    main::DatabaseManager* databaseManager;

    ClearCacheBindData(main::DatabaseManager* databaseManager,
        std::vector<common::LogicalType> returnTypes, std::vector<std::string> returnColumnNames,
        common::offset_t maxOffset)
        : CallTableFuncBindData{std::move(returnTypes), std::move(returnColumnNames), maxOffset},
          databaseManager{databaseManager} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<ClearCacheBindData>(databaseManager,
            common::LogicalType::copy(columnTypes), columnNames, maxOffset);
    }
};

struct ClearCacheFunction final : public function::TableFunction {
    static constexpr const char* name = "clear_attached_db_cache";

    ClearCacheFunction();
};

} // namespace duckdb_extension
} // namespace kuzu
