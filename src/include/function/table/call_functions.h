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

    inline bool hasMoreToOutput() const { return startOffset != common::INVALID_OFFSET; }

    inline static CallFuncMorsel createInvalidMorsel() {
        return CallFuncMorsel{common::INVALID_OFFSET, common::INVALID_OFFSET};
    }
};

struct CallFuncSharedState : public TableFuncSharedState {
    common::offset_t maxOffset;
    common::offset_t curOffset;
    std::mutex mtx;

    explicit CallFuncSharedState(common::offset_t maxOffset) : maxOffset{maxOffset}, curOffset{0} {}

    KUZU_API CallFuncMorsel getMorsel();
};

struct CallTableFuncBindData : public TableFuncBindData {
    common::offset_t maxOffset;

    CallTableFuncBindData(std::vector<common::LogicalType> columnTypes,
        std::vector<std::string> returnColumnNames, common::offset_t maxOffset)
        : TableFuncBindData{std::move(columnTypes), std::move(returnColumnNames)},
          maxOffset{maxOffset} {}

    inline std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<CallTableFuncBindData>(common::LogicalType::copy(columnTypes),
            columnNames, maxOffset);
    }
};

struct KUZU_API CallFunction {
    static std::unique_ptr<TableFuncSharedState> initSharedState(TableFunctionInitInput& input);
    static std::unique_ptr<TableFuncLocalState> initEmptyLocalState(TableFunctionInitInput& input,
        TableFuncSharedState* state, storage::MemoryManager* mm);
};

struct CurrentSettingFunction : public CallFunction {
    static constexpr const char* name = "CURRENT_SETTING";

    static function_set getFunctionSet();
};

struct DBVersionFunction : public CallFunction {
    static constexpr const char* name = "DB_VERSION";

    static function_set getFunctionSet();
};

struct ShowTablesFunction : public CallFunction {
    static constexpr const char* name = "SHOW_TABLES";

    static function_set getFunctionSet();
};

struct TableInfoFunction : public CallFunction {
    static constexpr const char* name = "TABLE_INFO";

    static function_set getFunctionSet();
};

struct ShowSequencesFunction : public CallFunction {
    static constexpr const char* name = "SHOW_SEQUENCES";

    static function_set getFunctionSet();
};

struct ShowConnectionFunction final : public CallFunction {
    static constexpr const char* name = "SHOW_CONNECTION";

    static function_set getFunctionSet();
};

struct StorageInfoFunction final : public CallFunction {
    static constexpr const char* name = "STORAGE_INFO";

    static function_set getFunctionSet();
};

struct ShowAttachedDatabasesFunction final : public CallFunction {
    static constexpr const char* name = "SHOW_ATTACHED_DATABASES";

    static function_set getFunctionSet();
};

struct CheckpointFunction final : public CallFunction {
    static constexpr const char* name = "CHECKPOINT";

    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
