#pragma once

#include "function/scalar_function.h"
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

    CallFuncMorsel getMorsel();
};

struct CallTableFuncBindData : public TableFuncBindData {
    common::offset_t maxOffset;

    CallTableFuncBindData(std::vector<common::LogicalType> columnTypes,
        std::vector<std::string> returnColumnNames, common::offset_t maxOffset)
        : TableFuncBindData{std::move(columnTypes), std::move(returnColumnNames)}, maxOffset{
                                                                                       maxOffset} {}

    inline std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<CallTableFuncBindData>(columnTypes, columnNames, maxOffset);
    }
};

struct CallFunction {
    static std::unique_ptr<TableFuncSharedState> initSharedState(TableFunctionInitInput& input);
    static std::unique_ptr<TableFuncLocalState> initEmptyLocalState(
        TableFunctionInitInput& input, TableFuncSharedState* state, storage::MemoryManager* mm);
};

struct CurrentSettingFunction : public CallFunction {
    static function_set getFunctionSet();
};

struct DBVersionFunction : public CallFunction {
    static function_set getFunctionSet();
};

struct ShowTablesFunction : public CallFunction {
    static function_set getFunctionSet();
};

struct TableInfoFunction : public CallFunction {
    static function_set getFunctionSet();
};

struct ShowConnectionFunction final : public CallFunction {
    static function_set getFunctionSet();
};

struct StorageInfoFunction final : public CallFunction {
    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
