#pragma once

#include "function/function.h"
#include "function/table/bind_data.h"
#include "function/table_functions.h"

namespace kuzu {
namespace function {

struct CallFuncMorsel {
    common::offset_t startOffset;
    common::offset_t endOffset;

    CallFuncMorsel(common::offset_t startOffset, common::offset_t endOffset)
        : startOffset{startOffset}, endOffset{endOffset} {}

    bool hasMoreToOutput() const { return startOffset != common::INVALID_OFFSET; }

    static CallFuncMorsel createInvalidMorsel() {
        return CallFuncMorsel{common::INVALID_OFFSET, common::INVALID_OFFSET};
    }
};

struct CallFuncSharedState : TableFuncSharedState {
    common::offset_t maxOffset;
    common::offset_t curOffset;
    std::mutex mtx;

    explicit CallFuncSharedState(common::offset_t maxOffset) : maxOffset{maxOffset}, curOffset{0} {}

    KUZU_API CallFuncMorsel getMorsel();
};

struct CallTableFuncBindData : TableFuncBindData {
    common::offset_t maxOffset;

    CallTableFuncBindData(std::vector<common::LogicalType> columnTypes,
        std::vector<std::string> returnColumnNames, common::offset_t maxOffset)
        : TableFuncBindData{std::move(columnTypes), std::move(returnColumnNames), 0},
          maxOffset{maxOffset} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<CallTableFuncBindData>(common::LogicalType::copy(columnTypes),
            columnNames, maxOffset);
    }
};

struct StandaloneTableFuncBindData : public CallTableFuncBindData {
    StandaloneTableFuncBindData()
        : CallTableFuncBindData{std::vector<common::LogicalType>{}, std::vector<std::string>{}, 1} {
    }

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<StandaloneTableFuncBindData>();
    }
};

struct KUZU_API CallFunction {
    static std::unique_ptr<TableFuncSharedState> initSharedState(TableFunctionInitInput& input);
    static std::unique_ptr<TableFuncLocalState> initEmptyLocalState(TableFunctionInitInput& input,
        TableFuncSharedState* state, storage::MemoryManager* mm);
};

struct CurrentSettingFunction : CallFunction {
    static constexpr const char* name = "CURRENT_SETTING";

    static function_set getFunctionSet();
};

struct DBVersionFunction : CallFunction {
    static constexpr const char* name = "DB_VERSION";

    static function_set getFunctionSet();
};

struct ShowTablesFunction : CallFunction {
    static constexpr const char* name = "SHOW_TABLES";

    static function_set getFunctionSet();
};

struct ShowWarningsFunction : CallFunction {
    static constexpr const char* name = "SHOW_WARNINGS";

    static function_set getFunctionSet();
};

struct ClearWarningsFunction : CallFunction {
    static constexpr const char* name = "CLEAR_WARNINGS";

    static function_set getFunctionSet();
};

struct TableInfoFunction : CallFunction {
    static constexpr const char* name = "TABLE_INFO";

    static function_set getFunctionSet();
};

struct ShowSequencesFunction : CallFunction {
    static constexpr const char* name = "SHOW_SEQUENCES";

    static function_set getFunctionSet();
};

struct ShowConnectionFunction final : CallFunction {
    static constexpr const char* name = "SHOW_CONNECTION";

    static function_set getFunctionSet();
};

struct StorageInfoFunction final : CallFunction {
    static constexpr const char* name = "STORAGE_INFO";

    static function_set getFunctionSet();
};

struct StatsInfoFunction final : CallFunction {
    static constexpr const char* name = "STATS_INFO";

    static function_set getFunctionSet();
};

struct ShowAttachedDatabasesFunction final : CallFunction {
    static constexpr const char* name = "SHOW_ATTACHED_DATABASES";

    static function_set getFunctionSet();
};

struct ShowFunctionsFunction : public CallFunction {
    static constexpr const char* name = "SHOW_FUNCTIONS";

    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
