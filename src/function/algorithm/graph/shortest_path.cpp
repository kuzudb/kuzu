#include "binder/expression/expression_util.h"
#include "common/exception/binder.h"
#include "common/types/value/nested.h"
#include "function/algorithm/graph.h"
#include "function/algorithm/graph_functions.h"
#include "function/algorithm/on_disk_graph.h"
#include "function/algorithm/parallel_utils.h"
#include "function/algorithm/ife_morsel.h"
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

struct ShortestPathAlgoBindData : public CallTableFuncBindData {
    std::string nodeTable;
    std::string relTable;
    int64_t lowerBound;
    int64_t upperBound;
    common::offset_t srcOffset;
    main::ClientContext* context;

    ShortestPathAlgoBindData(std::vector<common::LogicalType> columnTypes,
        std::vector<std::string> columnNames, std::string nodeTable, std::string relTable,
        int64_t lowerBound, int64_t upperBound, common::offset_t srcOffset,
        main::ClientContext* context)
        : CallTableFuncBindData{std::move(columnTypes), std::move(columnNames),
              common::INVALID_OFFSET}, nodeTable{nodeTable}, relTable{relTable},
          lowerBound{lowerBound}, upperBound{upperBound}, srcOffset{srcOffset}, context{context} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<ShortestPathAlgoBindData>(
            columnTypes, columnNames, nodeTable, relTable, lowerBound, upperBound, srcOffset,
            context);
    }
};

// Bind input parameters.
// All parameters required at binding time:
// (1) node table name (string)
// (2) rel table name (string)
// (3) lower bound (int)
// (4) upper bound (int)
// (5) src offset of bfs start
// TODO: Currently hardcoded some src offset as input to function, it should be able to handle
// scanning and passing source offset to function.
static std::unique_ptr<TableFuncBindData> bindFunc(
    ClientContext* context, TableFuncBindInput* input) {
    if (input->inputs.size() != 5) {
        throw BinderException("Not enough parameters for Shortest Path function ...");
    }
    auto nodeTable = input->inputs[0].getValue<std::string>();
    auto relTable = input->inputs[1].getValue<std::string>();
    auto lowerBound = input->inputs[2].getValue<int64_t>();
    auto upperBound = input->inputs[3].getValue<int64_t>();
    auto srcOffset = input->inputs[4].getValue<uint64_t>();
    std::vector<LogicalType> columnTypes;
    std::vector<std::string> columnNames;
    columnTypes.push_back(*LogicalType::UINT64());
    columnTypes.push_back(*LogicalType::UINT64());
    columnTypes.push_back(*LogicalType::UINT64());
    columnNames.push_back("src_offset");
    columnNames.push_back("dst_offset");
    columnNames.push_back("path_length");
    return std::make_unique<ShortestPathAlgoBindData>( std::move(columnTypes), std::move(columnNames),
        nodeTable, relTable, lowerBound, upperBound, srcOffset, context);
}

struct ShortestPathAlgoSharedState : public CallFuncSharedState {
    std::unique_ptr<Graph> graph;
    std::unique_ptr<IFEMorsel> ifeMorsel;

    ShortestPathAlgoSharedState(common::offset_t maxOffset, common::offset_t curOffset,
        uint64_t morselSize, std::unique_ptr<IFEMorsel> ifeMorsel, std::unique_ptr<Graph> graph)
        : CallFuncSharedState{maxOffset, curOffset, morselSize}, graph{std::move(graph)},
          ifeMorsel{std::move(ifeMorsel)} {}

    CallFuncMorsel getMorsel() override {

    }
};

std::unique_ptr<TableFuncSharedState> shortestPathAlgoInitSharedState(TableFunctionInitInput& input) {
    auto bindData = ku_dynamic_cast<TableFuncBindData*, ShortestPathAlgoBindData*>(input.bindData);
    std::vector<std::string> graphTableNames;
    graphTableNames.push_back(bindData->nodeTable);
    graphTableNames.push_back(bindData->relTable);
    auto graph = std::make_unique<OnDiskGraph>(bindData->context, graphTableNames);
    auto maxOffset = graph->getNumNodes();
    auto ifeMorsel = std::make_unique<IFEMorsel>(bindData->upperBound, bindData->lowerBound,
        maxOffset);
    return std::make_unique<ShortestPathAlgoSharedState>(maxOffset, 0LU /* curOffset */,
        64LU /* hard-coding a morsel size for now*/, std::move(ifeMorsel), std::move(graph));
}

/*
 * This will be main function for logic, doing frontier extension and updating state.
 */
static common::offset_t tableFunc(TableFuncInput& input, TableFuncOutput& output) {
    auto sharedState =
        ku_dynamic_cast<TableFuncSharedState*, ShortestPathAlgoSharedState*>(input.sharedState);
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

bool ShortestPath::compute(Sink* sink, ExecutionContext* executionContext,
    std::shared_ptr<ParallelUtils> parallelUtils) {
    curFrontier.initToNode(source);
    while (curFrontier.notEmpty()) {
        parallelUtils->morselizeFrontier(curFrontier, BFUpdate);
        parallelUtils->initFrontierParallel(curFrontier, BFCalculateInFrontier, nextFrontier);
        swap(curDistances, nextDistances);
        swap(curFrontier, nextFrontier);
    }
}

function::function_set ShortestPath::getFunctionSet() {
    function_set functionSet;
    auto function = std::make_unique<TableFunction>(name, tableFunc, bindFunc,
        shortestPathAlgoInitSharedState, CallFunction::initEmptyLocalState,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING,
            LogicalTypeID::INT64, LogicalTypeID::INT64, LogicalTypeID::UINT64});
    functionSet.push_back(std::move(function));
    return functionSet;
}

}
}
