#pragma once

#include <utility>

#include "common/arrow/arrow.h"
#include "function/scalar_function.h"
#include "function/table/bind_data.h"
#include "function/table/scan_file_function.h"
#include "function/table/table_function.h"
#include "pybind_include.h"

namespace kuzu {

struct PyArrowTableScanLocalState final : public function::TableFuncLocalState {
    ArrowArrayWrapper* arrowArray;

    explicit PyArrowTableScanLocalState(ArrowArrayWrapper* arrowArray) : arrowArray{arrowArray} {}
};

struct PyArrowTableScanSharedState final : public function::TableFuncSharedState {
    std::vector<std::shared_ptr<ArrowArrayWrapper>> chunks;
    uint64_t currentChunk;

    PyArrowTableScanSharedState(uint64_t numRows,
        std::vector<std::shared_ptr<ArrowArrayWrapper>> chunks)
        : TableFuncSharedState{numRows}, chunks{std::move(chunks)}, currentChunk{0} {}

    ArrowArrayWrapper* getNextChunk();
};

struct PyArrowTableScanFunctionData final : public function::TableFuncBindData {
    std::shared_ptr<ArrowSchemaWrapper> schema;
    std::vector<std::shared_ptr<ArrowArrayWrapper>> arrowArrayBatches;
    bool ignoreErrors;

    PyArrowTableScanFunctionData(binder::expression_vector columns,
        std::shared_ptr<ArrowSchemaWrapper> schema,
        std::vector<std::shared_ptr<ArrowArrayWrapper>> arrowArrayBatches, uint64_t numRows,
        bool ignoreErrors)
        : TableFuncBindData{std::move(columns), numRows}, schema{std::move(schema)},
          arrowArrayBatches{std::move(arrowArrayBatches)}, ignoreErrors(ignoreErrors) {}

    ~PyArrowTableScanFunctionData() override {}

    bool getIgnoreErrorsOption() const override { return ignoreErrors; }

private:
    PyArrowTableScanFunctionData(const PyArrowTableScanFunctionData& other) = default;

public:
    std::unique_ptr<function::TableFuncBindData> copy() const override {
        py::gil_scoped_acquire acquire;
        // the schema is considered immutable so copying it by copying the shared_ptr is fine.
        return std::unique_ptr<PyArrowTableScanFunctionData>(
            new PyArrowTableScanFunctionData(*this));
    }
};

struct PyArrowTableScanFunction {
    static constexpr const char* name = "READ_PYARROW";

    static function::function_set getFunctionSet();

    static function::TableFunction getFunction();
};

} // namespace kuzu
