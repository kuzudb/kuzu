#pragma once

#include "function/scalar_function.h"
#include "function/table/bind_data.h"
#include "function/table/scan_functions.h"
#include "function/table_functions.h"
#include "pandas_bind.h"
#include "pybind_include.h"

namespace kuzu {

struct PandasScanLocalState final : public function::TableFuncLocalState {
    PandasScanLocalState(uint64_t start, uint64_t end) : start{start}, end{end} {}

    uint64_t start;
    uint64_t end;
};

struct PandasScanSharedState final : public function::BaseScanSharedStateWithNumRows {
    explicit PandasScanSharedState(uint64_t numRows)
        : BaseScanSharedStateWithNumRows{numRows}, numRowsRead{0} {}

    uint64_t numRowsRead;
};

struct PandasScanFunction {
    static constexpr const char* name = "READ_PANDAS";

    static function::function_set getFunctionSet();
    static function::TableFunction getFunction();
};

struct PandasScanFunctionData : public function::TableFuncBindData {
    py::handle df;
    std::vector<std::unique_ptr<PandasColumnBindData>> columnBindData;

    PandasScanFunctionData(binder::expression_vector columns, py::handle df, uint64_t numRows,
        std::vector<std::unique_ptr<PandasColumnBindData>> columnBindData)
        : TableFuncBindData{std::move(columns), 0 /* numWarningDataColumns */, numRows}, df{df},
          columnBindData{std::move(columnBindData)} {}

    ~PandasScanFunctionData() override {
        py::gil_scoped_acquire acquire;
        columnBindData.clear();
    }

    std::vector<std::unique_ptr<PandasColumnBindData>> copyColumnBindData() const;

    std::unique_ptr<function::TableFuncBindData> copy() const override {
        return std::unique_ptr<PandasScanFunctionData>(new PandasScanFunctionData(*this));
    }

private:
    PandasScanFunctionData(const PandasScanFunctionData& other)
        : TableFuncBindData{other}, df{other.df} {
        for (const auto& i : other.columnBindData) {
            columnBindData.push_back(i->copy());
        }
    }
};

std::unique_ptr<function::ScanReplacementData> tryReplacePD(py::dict& dict, py::str& objectName);

} // namespace kuzu
