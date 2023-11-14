#pragma once

#include "function/scalar_function.h"
#include "function/table_functions.h"
#include "function/table_functions/bind_data.h"
#include "function/table_functions/scan_functions.h"
#include "pandas_bind.h"
#include "pybind_include.h"

namespace kuzu {

struct PandasScanLocalState : public function::TableFuncLocalState {
    PandasScanLocalState(uint64_t start, uint64_t end) : start{start}, end{end} {}

    uint64_t start;
    uint64_t end;
};

struct PandasScanSharedState : public function::BaseScanSharedState {
    explicit PandasScanSharedState(uint64_t numRows) : BaseScanSharedState{numRows}, position{0} {}

    std::mutex lock;
    uint64_t position;
};

struct PandasScanFunction {
    static function::function_set getFunctionSet();

    static void tableFunc(function::TableFunctionInput& input, common::DataChunk& outputChunk);

    static std::unique_ptr<function::TableFuncBindData> bindFunc(main::ClientContext* /*context*/,
        function::TableFuncBindInput* input, catalog::CatalogContent* catalog);

    static std::unique_ptr<function::TableFuncSharedState> initSharedState(
        function::TableFunctionInitInput& input);

    static std::unique_ptr<function::TableFuncLocalState> initLocalState(
        function::TableFunctionInitInput& input, function::TableFuncSharedState* state);

    static bool sharedStateNext(const function::TableFuncBindData* bindData,
        PandasScanLocalState* localState, function::TableFuncSharedState* sharedState);

    static void pandasBackendScanSwitch(PandasColumnBindData* bindData, uint64_t count,
        uint64_t offset, common::ValueVector* outputVector);
};

struct PandasScanFunctionData : public function::TableFuncBindData {
    py::handle df;
    uint64_t numRows;
    std::vector<std::unique_ptr<PandasColumnBindData>> columnBindData;

    PandasScanFunctionData(std::vector<std::unique_ptr<common::LogicalType>> columnTypes,
        std::vector<std::string> columnNames, py::handle df, uint64_t numRows,
        std::vector<std::unique_ptr<PandasColumnBindData>> columnBindData)
        : TableFuncBindData{std::move(columnTypes), std::move(columnNames)}, df{df},
          numRows{numRows}, columnBindData{std::move(columnBindData)} {}

    ~PandasScanFunctionData() override {
        py::gil_scoped_acquire acquire;
        columnBindData.clear();
    }

    std::vector<std::unique_ptr<PandasColumnBindData>> copyColumnBindData();

    std::unique_ptr<function::TableFuncBindData> copy() override {
        return std::make_unique<PandasScanFunctionData>(
            common::LogicalType::copy(columnTypes), columnNames, df, numRows, copyColumnBindData());
    }
};

std::unique_ptr<common::Value> replacePD(common::Value* value);

} // namespace kuzu
