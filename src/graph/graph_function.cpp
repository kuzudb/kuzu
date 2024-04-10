#include "graph/graph_function.h"

#include "binder/expression/expression_util.h"
#include "common/exception/binder.h"
#include "common/types/value/nested.h"
#include "function/table/bind_data.h"
#include "function/table/bind_input.h"
#include "function/table/call_functions.h"
#include "function/table_functions.h"
#include "graph/graph.h"
#include "graph/on_disk_graph.h"

using namespace kuzu::function;
using namespace kuzu::main;
using namespace kuzu::common;
using namespace kuzu::binder;

namespace kuzu {
namespace graph {

struct DemoAlgoBindData : public CallTableFuncBindData {
    main::ClientContext* context;
    std::vector<std::string> graphTableNames;

    DemoAlgoBindData(std::vector<common::LogicalType> columnTypes,
        std::vector<std::string> columnNames, common::offset_t maxOffset,
        main::ClientContext* context, std::vector<std::string> graphTableNames)
        : CallTableFuncBindData{std::move(columnTypes), std::move(columnNames), maxOffset},
          context{context}, graphTableNames{std::move(graphTableNames)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<DemoAlgoBindData>(
            columnTypes, columnNames, maxOffset, context, graphTableNames);
    }
};

struct DemoAlgoSharedState : public CallFuncSharedState {
    std::unique_ptr<Graph> graph;

    DemoAlgoSharedState(common::offset_t maxOffset, std::unique_ptr<Graph> graph)
        : CallFuncSharedState{maxOffset}, graph{std::move(graph)} {}
};

std::unique_ptr<TableFuncSharedState> demoAlgoInitSharedState(TableFunctionInitInput& input) {
    auto bindData = ku_dynamic_cast<TableFuncBindData*, DemoAlgoBindData*>(input.bindData);
    auto graph = std::make_unique<OnDiskGraph>(bindData->context, bindData->graphTableNames);
    return std::make_unique<DemoAlgoSharedState>(bindData->maxOffset, std::move(graph));
}

static common::offset_t tableFunc(TableFuncInput& input, TableFuncOutput& output) {
    auto sharedState =
        ku_dynamic_cast<TableFuncSharedState*, DemoAlgoSharedState*>(input.sharedState);
    auto morsel = sharedState->getMorsel();
    if (!morsel.hasMoreToOutput()) {
        return 0;
    }
    auto graph = sharedState->graph.get();
    double numNodes = graph->getNumNodes();
    double numEdges = graph->getNumEdges();
    numEdges = numEdges == 0 ? 1 : numEdges;
    double degree = numEdges / numNodes;
    auto vector = output.dataChunk.valueVectors[0].get();
    vector->state->selVector->selectedSize = 1;
    vector->setValue<double>(0, degree);
    return 1;
}

// TODO(Xiyang): I would just pass literal expression in TableFuncBindInput instead.
static void validateValueType(const common::Value& value, LogicalTypeID targetTypeID) {
    if (value.getDataType()->getLogicalTypeID() == targetTypeID) {
        return;
    }
    throw BinderException("X");
}

static std::vector<std::string> extractTableNamesFromListValue(const Value& value) {
    std::vector<std::string> tableNames;
    for (auto i = 0u; i < NestedVal::getChildrenSize(&value); ++i) {
        auto entry = NestedVal::getChildVal(&value, i);
        validateValueType(*entry, LogicalTypeID::STRING);
        tableNames.push_back(entry->getValue<std::string>());
    }
    return tableNames;
}

// Bind input parameters.
static std::unique_ptr<TableFuncBindData> bindFunc(
    ClientContext* context, TableFuncBindInput* input) {
    if (input->inputs.size() != 1) {
        throw BinderException("Demo algorithm ...");
    }
    validateValueType(input->inputs[0], LogicalTypeID::LIST);
    auto graphTableNames = extractTableNamesFromListValue(input->inputs[0]);
    std::vector<LogicalType> columnTypes;
    std::vector<std::string> columnNames;
    columnTypes.push_back(*LogicalType::DOUBLE());
    columnNames.push_back("degree");
    return std::make_unique<DemoAlgoBindData>(
        std::move(columnTypes), std::move(columnNames), 1, context, std::move(graphTableNames));
}

function::function_set DemoAlgorithm::getFunctionSet() {
    function_set functionSet;
    auto function =
        std::make_unique<TableFunction>(name, tableFunc, bindFunc, demoAlgoInitSharedState,
            CallFunction::initEmptyLocalState, std::vector<LogicalTypeID>{LogicalTypeID::LIST});
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace graph
} // namespace kuzu
