#include "pyarrow/pyarrow_scan.h"

#include "cached_import/py_cached_import.h"
#include "common/arrow/arrow_converter.h"
#include "function/cast/functions/numeric_limits.h"
#include "function/table/bind_input.h"
#include "py_connection.h"
#include "pybind11/pytypes.h"

using namespace kuzu::function;
using namespace kuzu::common;
using namespace kuzu::catalog;

namespace kuzu {

PyArrowScanConfig::PyArrowScanConfig(const std::unordered_map<std::string, Value>& options) {
    skipNum = 0;
    limitNum = NumericLimits<uint64_t>::maximum();
    for (const auto& i : options) {
        if (i.first == "SKIP") {
            if (i.second.getDataType().getLogicalTypeID() != LogicalTypeID::INT64 ||
                i.second.val.int64Val < 0) {
                throw BinderException("SKIP Option must be a positive integer literal.");
            }
            skipNum = i.second.val.int64Val;
        } else if (i.first == "LIMIT") {
            if (i.second.getDataType().getLogicalTypeID() != LogicalTypeID::INT64 ||
                i.second.val.int64Val < 0) {
                throw BinderException("LIMIT Option must be a positive integer literal.");
            }
            limitNum = i.second.val.int64Val;
        } else {
            throw BinderException(stringFormat("{} Option not recognized by pyArrow scanner."));
        }
    }
}

static std::unique_ptr<function::TableFuncBindData> bindFunc(main::ClientContext* /*context*/,
    ScanTableFuncBindInput* input) {
    // TODO: This binding step could use some drastic improvements.
    // Particularly when scanning from pandas or polars.
    // Possibly look into using the pycapsule interface.
    py::gil_scoped_acquire acquire;
    py::object table(py::reinterpret_borrow<py::object>(
        reinterpret_cast<PyObject*>(input->inputs[0].getValue<uint8_t*>())));
    if (PyConnection::isPandasDataframe(table)) {
        table = importCache->pyarrow.lib.Table.from_pandas()(table);
    } else if (py::isinstance(table, importCache->polars.DataFrame())) {
        table = table.attr("to_arrow")();
    }
    std::vector<LogicalType> returnTypes;
    std::vector<std::string> names;
    if (py::isinstance<py::dict>(table)) {
        KU_UNREACHABLE;
    }
    auto numRows = py::len(table);
    auto schema = Pyarrow::bind(table, returnTypes, names);
    auto config = PyArrowScanConfig(input->config.options);
    // The following python operations are zero copy as defined in pyarrow docs.
    if (config.skipNum != 0) {
        table = table.attr("slice")(config.skipNum);
    }
    if (config.limitNum != NumericLimits<uint64_t>::maximum()) {
        table = table.attr("slice")(0, config.limitNum);
    }
    py::list batches = table.attr("to_batches")(DEFAULT_VECTOR_CAPACITY);
    std::vector<std::shared_ptr<ArrowArrayWrapper>> arrowArrayBatches;
    for (auto& i : batches) {
        arrowArrayBatches.push_back(std::make_shared<ArrowArrayWrapper>());
        i.attr("_export_to_c")(reinterpret_cast<uint64_t>(arrowArrayBatches.back().get()));
    }

    return std::make_unique<PyArrowTableScanFunctionData>(std::move(returnTypes), std::move(schema),
        std::move(names), arrowArrayBatches, numRows);
}

ArrowArrayWrapper* PyArrowTableScanSharedState::getNextChunk() {
    std::lock_guard<std::mutex> lck{lock};
    if (currentChunk == chunks.size()) {
        return nullptr;
    }
    return chunks[currentChunk++].get();
}

static std::unique_ptr<function::TableFuncSharedState> initSharedState(
    function::TableFunctionInitInput& input) {

    py::gil_scoped_acquire acquire;
    PyArrowTableScanFunctionData* bindData =
        dynamic_cast<PyArrowTableScanFunctionData*>(input.bindData);

    return std::make_unique<PyArrowTableScanSharedState>(bindData->numRows,
        bindData->arrowArrayBatches);
}

static std::unique_ptr<function::TableFuncLocalState> initLocalState(
    function::TableFunctionInitInput& /*input*/, function::TableFuncSharedState* sharedState,
    storage::MemoryManager* /*mm*/) {

    PyArrowTableScanSharedState* pyArrowShared =
        dynamic_cast<PyArrowTableScanSharedState*>(sharedState);
    return std::make_unique<PyArrowTableScanLocalState>(pyArrowShared->getNextChunk());
}

static common::offset_t tableFunc(function::TableFuncInput& input,
    function::TableFuncOutput& output) {

    auto arrowScanData = dynamic_cast<PyArrowTableScanFunctionData*>(input.bindData);
    auto arrowLocalState = dynamic_cast<PyArrowTableScanLocalState*>(input.localState);
    auto arrowSharedState = dynamic_cast<PyArrowTableScanSharedState*>(input.sharedState);
    if (arrowLocalState->arrowArray == nullptr) {
        return 0;
    }
    auto skipCols = arrowScanData->getColumnSkips();
    for (auto i = 0u; i < arrowScanData->columnTypes.size(); i++) {
        if (!skipCols[i]) {
            common::ArrowConverter::fromArrowArray(arrowScanData->schema->children[i],
                arrowLocalState->arrowArray->children[i],
                output.dataChunk.getValueVectorMutable(i));
        }
    }
    auto len = arrowLocalState->arrowArray->length;
    arrowLocalState->arrowArray = arrowSharedState->getNextChunk();
    return len;
}

static double progressFunc(function::TableFuncSharedState* sharedState) {
    PyArrowTableScanSharedState* state = ku_dynamic_cast<PyArrowTableScanSharedState*>(sharedState);
    if (state->chunks.size() == 0) {
        return 0.0;
    }
    return static_cast<double>(state->currentChunk) / state->chunks.size();
}

function::function_set PyArrowTableScanFunction::getFunctionSet() {

    function_set functionSet;
    functionSet.push_back(
        std::make_unique<TableFunction>(name, tableFunc, bindFunc, initSharedState, initLocalState,
            progressFunc, std::vector<LogicalTypeID>{LogicalTypeID::POINTER}));
    return functionSet;
}

TableFunction PyArrowTableScanFunction::getFunction() {
    return TableFunction(name, tableFunc, bindFunc, initSharedState, initLocalState, progressFunc,
        std::vector<LogicalTypeID>{LogicalTypeID::POINTER});
}

} // namespace kuzu
