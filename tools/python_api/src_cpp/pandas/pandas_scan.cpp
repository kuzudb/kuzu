#include "pandas/pandas_scan.h"

#include "pyarrow/pyarrow_scan.h"
#include "function/table/bind_input.h"
#include "cached_import/py_cached_import.h"
#include "numpy/numpy_scan.h"
#include "py_connection.h"
#include "pybind11/pytypes.h"
#include "binder/expression/function_expression.h"

using namespace kuzu::function;
using namespace kuzu::common;
using namespace kuzu::catalog;

namespace kuzu {

std::unique_ptr<function::TableFuncBindData> bindFunc(
    main::ClientContext* /*context*/, TableFuncBindInput* input) {
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
    return std::make_unique<PandasScanFunctionData>(
        std::move(returnTypes), std::move(names), df, numRows, std::move(columnBindData));
}

bool sharedStateNext(const TableFuncBindData* /*bindData*/,
    PandasScanLocalState* localState, TableFuncSharedState* sharedState) {
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
    auto scanBindData = reinterpret_cast<PandasScanFunctionData*>(input.bindData);
    return std::make_unique<PandasScanSharedState>(scanBindData->numRows);
}

void pandasBackendScanSwitch(
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

offset_t tableFunc(
    TableFuncInput& input, TableFuncOutput& output) {
    auto pandasScanData = reinterpret_cast<PandasScanFunctionData*>(input.bindData);
    auto pandasLocalState = reinterpret_cast<PandasScanLocalState*>(input.localState);

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
    output.dataChunk.state->selVector->selectedSize = numValuesToOutput;
    pandasLocalState->start += numValuesToOutput;
    return numValuesToOutput;
}

std::vector<std::unique_ptr<PandasColumnBindData>> PandasScanFunctionData::copyColumnBindData() const {
    std::vector<std::unique_ptr<PandasColumnBindData>> result;
    result.reserve(columnBindData.size());
    for (auto& bindData : columnBindData) {
        result.push_back(bindData->copy());
    }
    return result;
}

static TableFunction getFunction() {
    return TableFunction(READ_PANDAS_FUNC_NAME, tableFunc, bindFunc, initSharedState,
        initLocalState, std::vector<LogicalTypeID>{LogicalTypeID::POINTER});
}

function_set PandasScanFunction::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(getFunction().copy());
    return functionSet;
}

static bool isPyArrowBacked(const py::handle &df) {
	py::list dtypes = df.attr("dtypes");
	if (dtypes.empty()) {
		return false;
	}

	auto arrow_dtype = importCache->pandas.ArrowDtype();
	for (auto &dtype : dtypes) {
		if (py::isinstance(dtype, arrow_dtype)) {
			return true;
		}
	}
	return false;
}

static std::unique_ptr<ScanReplacementData> tryReplacePD(py::dict& dict, py::str& objectName) {
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
        throw RuntimeException{"Only pandas dataframe is supported."};
    }
}

std::unique_ptr<ScanReplacementData> replacePD(const std::string& objectName) {
    auto pyTableName = py::str(objectName);
    // Here we do an exhaustive search on the frame lineage.
    py::gil_scoped_acquire acquire;
    auto currentFrame = importCache->inspect.currentframe()();
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
    return nullptr;
}

} // namespace kuzu
