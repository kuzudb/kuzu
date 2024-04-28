#pragma once

#include "common/arrow/arrow.h"
#include "function/scalar_function.h"
#include "function/table/bind_data.h"
#include "function/table/scan_functions.h"
#include "function/table_functions.h"
#include "pyarrow_bind.h"
#include "pybind_include.h"

namespace kuzu {

struct PyArrowTableScanLocalState final : public function::TableFuncLocalState {
    ArrowArrayWrapper* arrowArray;

    explicit PyArrowTableScanLocalState(ArrowArrayWrapper* arrowArray) : arrowArray{arrowArray} {}
};

struct PyArrowTableScanSharedState final : public function::BaseFileScanSharedState {
    std::vector<std::shared_ptr<ArrowArrayWrapper>> chunks;
    uint64_t currentChunk;
    std::mutex lock;

    PyArrowTableScanSharedState(uint64_t numRows,
        std::vector<std::shared_ptr<ArrowArrayWrapper>> chunks)
        : BaseFileScanSharedState{numRows}, chunks{std::move(chunks)}, currentChunk{0} {}

    ArrowArrayWrapper* getNextChunk();
};

struct PyArrowTableScanFunctionData final : public function::TableFuncBindData {
    std::shared_ptr<ArrowSchemaWrapper> schema;
    std::vector<std::shared_ptr<ArrowArrayWrapper>> arrowArrayBatches;
    uint64_t numRows;

    PyArrowTableScanFunctionData(std::vector<common::LogicalType> columnTypes,
        std::shared_ptr<ArrowSchemaWrapper> schema, std::vector<std::string> columnNames,
        std::vector<std::shared_ptr<ArrowArrayWrapper>> arrowArrayBatches, uint64_t numRows)
        : TableFuncBindData{std::move(columnTypes), std::move(columnNames)},
          schema{std::move(schema)}, arrowArrayBatches{arrowArrayBatches}, numRows{numRows} {}

    ~PyArrowTableScanFunctionData() override {}

    std::unique_ptr<function::TableFuncBindData> copy() const override {
        py::gil_scoped_acquire acquire;
        // the schema is considered immutable so copying it by copying the shared_ptr is fine.
        return std::make_unique<PyArrowTableScanFunctionData>(columnTypes, schema, columnNames,
            arrowArrayBatches, numRows);
    }
};

struct PyArrowTableScanFunction {
    static constexpr const char* name = "READ_PYARROW";

    static function::function_set getFunctionSet();

    static function::TableFunction getFunction();
};

} // namespace kuzu
