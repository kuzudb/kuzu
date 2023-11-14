#include "pandas/pandas_scan.h"

#include "function/table_functions/bind_input.h"
#include "numpy/numpy_scan.h"
#include "py_connection.h"
#include "pybind11/pytypes.h"

using namespace kuzu::function;
using namespace kuzu::common;
using namespace kuzu::catalog;

namespace kuzu {

function_set PandasScanFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(
        std::make_unique<TableFunction>(READ_PANDAS_FUNC_NAME, tableFunc, bindFunc, initSharedState,
            initLocalState, std::vector<LogicalTypeID>{LogicalTypeID::POINTER}));
    return functionSet;
}

std::unique_ptr<function::TableFuncBindData> PandasScanFunction::bindFunc(
    main::ClientContext* /*context*/, TableFuncBindInput* input, CatalogContent* /*catalog*/) {
    py::gil_scoped_acquire acquire;
    py::handle df(reinterpret_cast<PyObject*>(input->inputs[0]->getValue<uint8_t*>()));
    std::vector<std::unique_ptr<PandasColumnBindData>> columnBindData;
    std::vector<std::unique_ptr<LogicalType>> returnTypes;
    std::vector<std::string> names;
    if (py::isinstance<py::dict>(df)) {
        KU_UNREACHABLE;
    } else {
        Pandas::bind(df, columnBindData, returnTypes, names);
    }
    auto columns = py::list(df.attr("keys")());
    auto getFunc = df.attr("__getitem__");
    auto numRows = py::len(getFunc(columns[0]));
    return std::make_unique<PandasScanFunctionData>(
        std::move(returnTypes), std::move(names), df, numRows, std::move(columnBindData));
}

bool PandasScanFunction::sharedStateNext(const TableFuncBindData* /*bindData*/,
    PandasScanLocalState* localState, function::TableFuncSharedState* sharedState) {
    auto pandasSharedState = reinterpret_cast<PandasScanSharedState*>(sharedState);
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

std::unique_ptr<function::TableFuncLocalState> PandasScanFunction::initLocalState(
    function::TableFunctionInitInput& input, function::TableFuncSharedState* sharedState) {
    auto localState = std::make_unique<PandasScanLocalState>(0 /* start */, 0 /* end */);
    sharedStateNext(input.bindData, localState.get(), sharedState);
    return std::move(localState);
}

std::unique_ptr<function::TableFuncSharedState> PandasScanFunction::initSharedState(
    function::TableFunctionInitInput& input) {
    // LCOV_EXCL_START
    if (PyGILState_Check()) {
        throw common::RuntimeException("PandasScan called but GIL was already held!");
    }
    // LCOV_EXCL_STOP
    auto scanBindData = reinterpret_cast<PandasScanFunctionData*>(input.bindData);
    return std::make_unique<PandasScanSharedState>(scanBindData->numRows);
}

void PandasScanFunction::pandasBackendScanSwitch(
    PandasColumnBindData* bindData, uint64_t count, uint64_t offset, ValueVector* outputVector) {
    auto backend = bindData->pandasCol->getBackEnd();
    switch (backend) {
    case PandasColumnBackend::NUMPY: {
        NumpyScan::scan(bindData, count, offset, outputVector);
    } break;
    default:
        KU_UNREACHABLE;
    }
}

void PandasScanFunction::tableFunc(
    function::TableFunctionInput& input, common::DataChunk& outputChunk) {
    auto pandasScanData = reinterpret_cast<PandasScanFunctionData*>(input.bindData);
    auto pandasLocalState = reinterpret_cast<PandasScanLocalState*>(input.localState);

    if (pandasLocalState->start >= pandasLocalState->end) {
        if (!sharedStateNext(input.bindData, pandasLocalState, input.sharedState)) {
            return;
        }
    }
    auto numValuesToOutput =
        std::min(DEFAULT_VECTOR_CAPACITY, pandasLocalState->end - pandasLocalState->start);
    for (auto i = 0u; i < pandasScanData->columnNames.size(); i++) {
        pandasBackendScanSwitch(pandasScanData->columnBindData[i].get(), numValuesToOutput,
            pandasLocalState->start, outputChunk.getValueVector(i).get());
    }
    outputChunk.state->selVector->selectedSize = numValuesToOutput;
    pandasLocalState->start += numValuesToOutput;
}

std::vector<std::unique_ptr<PandasColumnBindData>> PandasScanFunctionData::copyColumnBindData() {
    std::vector<std::unique_ptr<PandasColumnBindData>> result;
    result.reserve(columnBindData.size());
    for (auto& bindData : columnBindData) {
        result.push_back(bindData->copy());
    }
    return result;
}

std::unique_ptr<Value> tryReplacePD(py::dict& dict, py::str& tableName) {
    if (!dict.contains(tableName)) {
        return nullptr;
    }
    auto entry = dict[tableName];
    if (PyConnection::isPandasDataframe(entry)) {
        return Value::createValue(reinterpret_cast<uint8_t*>(entry.ptr())).copy();
    } else {
        throw RuntimeException{"Only pandas dataframe is supported."};
    }
}

std::unique_ptr<common::Value> replacePD(common::Value* value) {
    py::gil_scoped_acquire acquire;
    auto pyTableName = py::str(value->getValue<std::string>());
    // Here we do an exhaustive search on the frame lineage.
    auto currentFrame = py::module::import("inspect").attr("currentframe")();
    while (hasattr(currentFrame, "f_locals")) {
        auto localDict = py::reinterpret_borrow<py::dict>(currentFrame.attr("f_locals"));
        if (localDict) {
            auto result = tryReplacePD(localDict, pyTableName);
            if (result) {
                return result;
            }
        }
        auto globalDict = py::reinterpret_borrow<py::dict>(currentFrame.attr("f_globals"));
        if (globalDict) {
            auto result = tryReplacePD(globalDict, pyTableName);
            if (result) {
                return result;
            }
        }
        currentFrame = currentFrame.attr("f_back");
    }
    return value->copy();
}

} // namespace kuzu
