#include "pyarrow/pyarrow_scan.h"

#include "binder/binder.h"
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

PyArrowScanConfig::PyArrowScanConfig(const common::case_insensitive_map_t<Value>& options) {
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

template<typename T>
static bool moduleIsLoaded() {
    auto dict = pybind11::module_::import("sys").attr("modules");
    return dict.contains(py::str(T::name_));
}

static std::unique_ptr<function::TableFuncBindData> bindFunc(main::ClientContext* /*context*/,
    TableFuncBindInput* input) {
    auto scanInput = ku_dynamic_cast<ExtraScanTableFuncBindInput*>(input->extraInput.get());
    // TODO: This binding step could use some drastic improvements.
    // Particularly when scanning from pandas or polars.
    // Possibly look into using the pycapsule interface.
    py::gil_scoped_acquire acquire;
    py::object table(py::reinterpret_borrow<py::object>(
        reinterpret_cast<PyObject*>(input->getLiteralVal<uint8_t*>(0))));
    if (PyConnection::isPandasDataframe(table)) {
        table = importCache->pyarrow.lib.Table.from_pandas()(table);
    } else if (moduleIsLoaded<PolarsCachedItem>() &&
               py::isinstance(table, importCache->polars.DataFrame())) {
        table = table.attr("to_arrow")();
    }
    std::vector<LogicalType> returnTypes;
    std::vector<std::string> names;
    if (py::isinstance<py::dict>(table)) {
        KU_UNREACHABLE;
    }
    auto numRows = py::len(table);
    auto schema = Pyarrow::bind(table, returnTypes, names);
    auto config = PyArrowScanConfig(scanInput->config.options);
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

    auto columns = input->binder->createVariables(names, returnTypes);
    return std::make_unique<PyArrowTableScanFunctionData>(std::move(columns), std::move(schema),
        arrowArrayBatches, numRows);
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

    return std::make_unique<PyArrowTableScanSharedState>(bindData->cardinality,
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
    for (auto i = 0u; i < arrowScanData->getNumColumns(); i++) {
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
    functionSet.push_back(getFunction().copy());
    return functionSet;
}

TableFunction PyArrowTableScanFunction::getFunction() {
    auto function = TableFunction(name, std::vector<LogicalTypeID>{LogicalTypeID::POINTER});
    function.tableFunc = tableFunc;
    function.bindFunc = bindFunc;
    function.initSharedStateFunc = initSharedState;
    function.initLocalStateFunc = initLocalState;
    function.progressFunc = progressFunc;
    return function;
}

} // namespace kuzu
