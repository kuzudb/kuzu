#pragma once

#include "function/function.h"
#include "function/table/bind_data.h"
#include "function/table_functions.h"

namespace kuzu {
namespace function {

struct SimpleTableFuncMorsel {
    common::offset_t startOffset;
    common::offset_t endOffset;

    SimpleTableFuncMorsel(common::offset_t startOffset, common::offset_t endOffset)
        : startOffset{startOffset}, endOffset{endOffset} {}

    bool hasMoreToOutput() const { return startOffset != common::INVALID_OFFSET; }

    static SimpleTableFuncMorsel createInvalidMorsel() {
        return {common::INVALID_OFFSET, common::INVALID_OFFSET};
    }

    bool isInvalid() const {
        return startOffset == common::INVALID_OFFSET && endOffset == common::INVALID_OFFSET;
    }
};

struct KUZU_API SimpleTableFuncSharedState : TableFuncSharedState {
    common::offset_t maxOffset;
    common::offset_t curOffset;
    std::mutex mtx;

    ~SimpleTableFuncSharedState() override;

    explicit SimpleTableFuncSharedState(common::offset_t maxOffset)
        : maxOffset{maxOffset}, curOffset{0} {}

    SimpleTableFuncMorsel getMorsel();
};

struct KUZU_API SimpleTableFuncBindData : TableFuncBindData {
    common::offset_t maxOffset;

    explicit SimpleTableFuncBindData(common::offset_t maxOffset)
        : SimpleTableFuncBindData{binder::expression_vector{}, maxOffset} {}
    SimpleTableFuncBindData(binder::expression_vector columns, common::offset_t maxOffset)
        : TableFuncBindData{std::move(columns)}, maxOffset{maxOffset} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<SimpleTableFuncBindData>(columns, maxOffset);
    }
};

/*
 * SimpleTableFunction outputs a fixed size table (can be zero).
 * */
struct KUZU_API SimpleTableFunction {
    static std::unique_ptr<TableFuncSharedState> initSharedState(
        const TableFunctionInitInput& input);
    static std::unique_ptr<TableFuncLocalState> initEmptyLocalState(
        const TableFunctionInitInput& input, TableFuncSharedState* state,
        storage::MemoryManager* mm);
    static std::vector<std::string> extractYieldVariables(const std::vector<std::string>& names,
        std::vector<parser::YieldVariable> yieldVariables);
};

struct CurrentSettingFunction final : SimpleTableFunction {
    static constexpr const char* name = "CURRENT_SETTING";

    static function_set getFunctionSet();
};

struct DBVersionFunction final : SimpleTableFunction {
    static constexpr const char* name = "DB_VERSION";

    static function_set getFunctionSet();
};

struct ShowTablesFunction final : SimpleTableFunction {
    static constexpr const char* name = "SHOW_TABLES";

    static function_set getFunctionSet();
};

struct ShowWarningsFunction final : SimpleTableFunction {
    static constexpr const char* name = "SHOW_WARNINGS";

    static function_set getFunctionSet();
};

struct ClearWarningsFunction final : SimpleTableFunction {
    static constexpr const char* name = "CLEAR_WARNINGS";

    static function_set getFunctionSet();
};

struct TableInfoFunction final : SimpleTableFunction {
    static constexpr const char* name = "TABLE_INFO";

    static function_set getFunctionSet();
};

struct ShowSequencesFunction final : SimpleTableFunction {
    static constexpr const char* name = "SHOW_SEQUENCES";

    static function_set getFunctionSet();
};

struct ShowConnectionFunction final : SimpleTableFunction {
    static constexpr const char* name = "SHOW_CONNECTION";

    static function_set getFunctionSet();
};

struct StorageInfoFunction final : SimpleTableFunction {
    static constexpr const char* name = "STORAGE_INFO";

    static function_set getFunctionSet();
};

struct StatsInfoFunction final : SimpleTableFunction {
    static constexpr const char* name = "STATS_INFO";

    static function_set getFunctionSet();
};

struct BMInfoFunction final : SimpleTableFunction {
    static constexpr const char* name = "BM_INFO";

    static function_set getFunctionSet();
};

struct ShowAttachedDatabasesFunction final : SimpleTableFunction {
    static constexpr const char* name = "SHOW_ATTACHED_DATABASES";

    static function_set getFunctionSet();
};

struct ShowFunctionsFunction final : SimpleTableFunction {
    static constexpr const char* name = "SHOW_FUNCTIONS";

    static function_set getFunctionSet();
};

struct CreateProjectGraphFunction final : SimpleTableFunction {
    static constexpr const char* name = "CREATE_PROJECT_GRAPH";

    static function_set getFunctionSet();
};

struct DropProjectGraphFunction final : SimpleTableFunction {
    static constexpr const char* name = "DROP_PROJECT_GRAPH";

    static function_set getFunctionSet();
};

struct ShowLoadedExtensionsFunction final : SimpleTableFunction {
    static constexpr const char* name = "SHOW_LOADED_EXTENSIONS";

    static function_set getFunctionSet();
};

struct ShowOfficialExtensionsFunction final : SimpleTableFunction {
    static constexpr const char* name = "SHOW_OFFICIAL_EXTENSIONS";

    static function_set getFunctionSet();
};

struct ShowIndexesFunction final : SimpleTableFunction {
    static constexpr const char* name = "SHOW_INDEXES";

    static function_set getFunctionSet();
};

} // namespace function
} // namespace kuzu
