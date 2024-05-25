#include "binder/expression/expression_util.h"
#include "common/exception/binder.h"
#include "common/types/value/nested.h"
#include "function/algorithm/graph.h"
#include "function/algorithm/graph_functions.h"
#include "function/algorithm/on_disk_graph.h"
#include "function/algorithm/parallel_utils.h"
#include "function/table/bind_data.h"
#include "function/table/bind_input.h"
#include "function/table/call_functions.h"
#include "function/table_functions.h"

using namespace kuzu::function;
using namespace kuzu::main;
using namespace kuzu::common;
using namespace kuzu::binder;

namespace kuzu {
namespace graph {

struct DemoAlgoBindData : public CallTableFuncBindData {
    main::ClientContext* context;
    std::vector<std::string> graphTableNames;
    common::offset_t curOffset;

    DemoAlgoBindData(std::vector<common::LogicalType> columnTypes,
        std::vector<std::string> columnNames, common::offset_t maxOffset,
        common::offset_t curOffset, main::ClientContext* context,
        std::vector<std::string> graphTableNames)
        : CallTableFuncBindData{std::move(columnTypes), std::move(columnNames), maxOffset},
          context{context}, graphTableNames{std::move(graphTableNames)}, curOffset{curOffset} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<DemoAlgoBindData>(
            columnTypes, columnNames, maxOffset, curOffset, context, graphTableNames);
    }
};

struct DemoAlgoSharedState : public CallFuncSharedState {
    std::unique_ptr<Graph> graph;

    DemoAlgoSharedState(common::offset_t maxOffset, common::offset_t curOffset, uint64_t morselSize,
        std::unique_ptr<Graph> graph)
        : CallFuncSharedState{maxOffset, curOffset, morselSize}, graph{std::move(graph)} {}
};

std::unique_ptr<TableFuncSharedState> demoAlgoInitSharedState(TableFunctionInitInput& input) {
    auto bindData = ku_dynamic_cast<TableFuncBindData*, DemoAlgoBindData*>(input.bindData);
    auto graph = std::make_unique<OnDiskGraph>(bindData->context, "" /* node table name */,
        bindData->graphTableNames[0] /* rel table name */);
    return std::make_unique<DemoAlgoSharedState>(
        bindData->maxOffset, bindData->curOffset, 1LU /* morsel size */, std::move(graph));
}

static common::offset_t tableFunc(TableFuncInput& input, TableFuncOutput& output) {
    auto sharedState =
        ku_dynamic_cast<TableFuncSharedState*, DemoAlgoSharedState*>(input.sharedState);
    auto morsel = sharedState->getMorsel();
    if (!morsel.hasMoreToOutput()) {
        return 0;
    }
    auto graph = sharedState->graph.get();
    auto offsetVector = output.dataChunk.valueVectors[0].get();
    auto degreeVector = output.dataChunk.valueVectors[1].get();
    offsetVector->state->selVector->selectedSize = (morsel.endOffset - morsel.startOffset);
    for (auto i = 0u; i < offsetVector->state->selVector->selectedSize; i++) {
        degreeVector->setValue(i, graph->getFwdDegreeOffset(morsel.startOffset));
        offsetVector->setValue(i, morsel.startOffset++);
    }
    return offsetVector->state->selVector->selectedSize;
}

// TODO(Xiyang): I would just pass literal expression in TableFuncBindInput instead.
static void validateValueType(const common::Value& value, LogicalTypeID targetTypeID) {
    if (value.getDataType()->getLogicalTypeID() == targetTypeID) {
        return;
    }
    throw BinderException("X");
}

// Bind input parameters.
static std::unique_ptr<TableFuncBindData> bindFunc(
    ClientContext* context, TableFuncBindInput* input) {
    if (input->inputs.size() != 3) {
        throw BinderException("Demo algorithm ...");
    }
    validateValueType(input->inputs[0], LogicalTypeID::STRING);
    validateValueType(input->inputs[1], LogicalTypeID::INT64);
    validateValueType(input->inputs[2], LogicalTypeID::INT64);
    auto graphTableNames = std::vector<std::string>({input->inputs[0].getValue<std::string>()});
    auto startOffset = input->inputs[1].getValue<int64_t>();
    auto endOffset = input->inputs[2].getValue<int64_t>();
    std::vector<LogicalType> columnTypes;
    std::vector<std::string> columnNames;
    columnTypes.push_back(*LogicalType::UINT64());
    columnTypes.push_back(*LogicalType::UINT64());
    columnNames.push_back("offset");
    columnNames.push_back("degree");
    return std::make_unique<DemoAlgoBindData>( std::move(columnTypes), std::move(columnNames),
        endOffset, startOffset, context, std::move(graphTableNames));
}

void DemoAlgorithm::compute(Sink *sink, ExecutionContext* executionContext,
    std::shared_ptr<ParallelUtils> parallelUtils) {
    parallelUtils->doParallel(sink, executionContext);
}

function::function_set DemoAlgorithm::getFunctionSet() {
    function_set functionSet;
    auto functionList = std::vector<table_func_t>({tableFunc});
    auto function = std::make_unique<TableFunction>(name, functionList, bindFunc,
        demoAlgoInitSharedState, CallFunction::initEmptyLocalState, std::vector<LogicalTypeID>{
            LogicalTypeID::STRING, LogicalTypeID::INT64, LogicalTypeID::INT64});
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace graph
} // namespace kuzu
