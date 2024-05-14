#include "pandas/pandas_scan.h"

#include "binder/expression/function_expression.h"
#include "cached_import/py_cached_import.h"
#include "common/exception/runtime.h"
#include "function/table/bind_input.h"
#include "numpy/numpy_scan.h"
#include "py_connection.h"
#include "pyarrow/pyarrow_scan.h"
#include "pybind11/pytypes.h"

using namespace kuzu::function;
using namespace kuzu::common;
using namespace kuzu::catalog;

namespace kuzu {

std::unique_ptr<function::TableFuncBindData> bindFunc(main::ClientContext* /*context*/,
    TableFuncBindInput* input) {
    py::gil_scoped_acquire acquire;
    py::handle df(reinterpret_cast<PyObject*>(input->inputs[0].getValue<uint8_t*>()));
    std::vector<std::unique_ptr<PandasColumnBindData>> columnBindData;
    std::vector<LogicalType> returnTypes;
    std::vector<std::string> names;
    if (py::isinstance<py::dict>(df)) {
        KU_UNREACHABLE;
    } else {
        Pandas::bind(df, columnBindData, returnTypes, names);
    }
    auto columns = py::list(df.attr("keys")());
    auto getFunc = df.attr("__getitem__");
    auto numRows = py::len(getFunc(columns[0]));
    return std::make_unique<PandasScanFunctionData>(std::move(returnTypes), std::move(names), df,
        numRows, std::move(columnBindData));
}

bool sharedStateNext(const TableFuncBindData* /*bindData*/, PandasScanLocalState* localState,
    TableFuncSharedState* sharedState) {
    auto pandasSharedState =
        ku_dynamic_cast<TableFuncSharedState*, PandasScanSharedState*>(sharedState);
    std::lock_guard<std::mutex> lck{pandasSharedState->lock};
    if (pandasSharedState->position >= pandasSharedState->numRows) {
        return false;
    }
    localState->start = pandasSharedState->position;
    pandasSharedState->position +=
        std::min(pandasSharedState->numRows - pandasSharedState->position,
            CopyConstants::PANDAS_PARTITION_COUNT);
    localState->end = pandasSharedState->position;
    return true;
}

std::unique_ptr<function::TableFuncLocalState> initLocalState(
    function::TableFunctionInitInput& input, function::TableFuncSharedState* sharedState,
    storage::MemoryManager* /*mm*/) {
    auto localState = std::make_unique<PandasScanLocalState>(0 /* start */, 0 /* end */);
    sharedStateNext(input.bindData, localState.get(), sharedState);
    return localState;
}

std::unique_ptr<function::TableFuncSharedState> initSharedState(
    function::TableFunctionInitInput& input) {
    // LCOV_EXCL_START
    if (PyGILState_Check()) {
        throw RuntimeException("PandasScan called but GIL was already held!");
    }
    // LCOV_EXCL_STOP
    auto scanBindData =
        ku_dynamic_cast<TableFuncBindData*, PandasScanFunctionData*>(input.bindData);
    return std::make_unique<PandasScanSharedState>(scanBindData->numRows);
}

void pandasBackendScanSwitch(PandasColumnBindData* bindData, uint64_t count, uint64_t offset,
    ValueVector* outputVector) {
    auto backend = bindData->pandasCol->getBackEnd();
    switch (backend) {
    case PandasColumnBackend::NUMPY: {
        NumpyScan::scan(bindData, count, offset, outputVector);
    } break;
    default:
        KU_UNREACHABLE;
    }
}

offset_t tableFunc(TableFuncInput& input, TableFuncOutput& output) {
    auto pandasScanData =
        ku_dynamic_cast<TableFuncBindData*, PandasScanFunctionData*>(input.bindData);
    auto pandasLocalState =
        ku_dynamic_cast<TableFuncLocalState*, PandasScanLocalState*>(input.localState);
    auto pandasSharedState =
        ku_dynamic_cast<TableFuncSharedState*, PandasScanSharedState*>(input.sharedState);

    if (pandasLocalState->start >= pandasLocalState->end) {
        if (!sharedStateNext(input.bindData, pandasLocalState, input.sharedState)) {
            return 0;
        }
    }
    auto numValuesToOutput =
        std::min(DEFAULT_VECTOR_CAPACITY, pandasLocalState->end - pandasLocalState->start);
    for (auto i = 0u; i < pandasScanData->columnNames.size(); i++) {
        pandasBackendScanSwitch(pandasScanData->columnBindData[i].get(), numValuesToOutput,
            pandasLocalState->start, output.dataChunk.getValueVector(i).get());
    }
    output.dataChunk.state->getSelVectorUnsafe().setSelSize(numValuesToOutput);
    pandasLocalState->start += numValuesToOutput;
    pandasSharedState->numReadRows += numValuesToOutput;
    return numValuesToOutput;
}

std::vector<std::unique_ptr<PandasColumnBindData>>
PandasScanFunctionData::copyColumnBindData() const {
    std::vector<std::unique_ptr<PandasColumnBindData>> result;
    result.reserve(columnBindData.size());
    for (auto& bindData : columnBindData) {
        result.push_back(bindData->copy());
    }
    return result;
}

static double progressFunc(TableFuncSharedState* sharedState) {
    auto pandasSharedState =
        ku_dynamic_cast<TableFuncSharedState*, PandasScanSharedState*>(sharedState);
    if (pandasSharedState->numRows == 0) {
        return 0.0;
    }
    return static_cast<double>(pandasSharedState->numReadRows) / pandasSharedState->numRows;
}

static TableFunction getFunction() {
    return TableFunction(PandasScanFunction::name, tableFunc, bindFunc, initSharedState,
        initLocalState, progressFunc, std::vector<LogicalTypeID>{LogicalTypeID::POINTER});
}

function_set PandasScanFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(getFunction().copy());
    return functionSet;
}

static bool isPyArrowBacked(const py::handle& df) {
    py::list dtypes = df.attr("dtypes");
    if (dtypes.empty()) {
        return false;
    }

    auto arrow_dtype = importCache->pandas.ArrowDtype();
    for (auto& dtype : dtypes) {
        if (py::isinstance(dtype, arrow_dtype)) {
            return true;
        }
    }
    return false;
}

std::unique_ptr<ScanReplacementData> tryReplacePD(py::dict& dict, py::str& objectName) {
    if (!dict.contains(objectName)) {
        return nullptr;
    }
    auto entry = dict[objectName];
    if (PyConnection::isPandasDataframe(entry)) {
        auto scanReplacementData = std::make_unique<ScanReplacementData>();
        if (isPyArrowBacked(entry)) {
            scanReplacementData->func = PyArrowTableScanFunction::getFunction();
        } else {
            scanReplacementData->func = getFunction();
        }
        auto bindInput = TableFuncBindInput();
        bindInput.inputs.push_back(Value::createValue(reinterpret_cast<uint8_t*>(entry.ptr())));
        scanReplacementData->bindInput = std::move(bindInput);
        return scanReplacementData;
    } else {
        return nullptr;
    }
}

} // namespace kuzu
