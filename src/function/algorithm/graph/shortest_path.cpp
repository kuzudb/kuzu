#include "binder/expression/expression_util.h"
#include "common/exception/binder.h"
#include "common/types/value/nested.h"
#include "function/algorithm/graph.h"
#include "function/algorithm/graph_functions.h"
#include "function/algorithm/ife_morsel.h"
#include "function/algorithm/on_disk_graph.h"
#include "function/algorithm/parallel_utils.h"
#include "function/table/bind_data.h"
#include "function/table/bind_input.h"
#include "function/table/call_functions.h"
#include "function/table_functions.h"
#include "processor/operator/algorithm/algorithm_runner.h"

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
              common::INVALID_OFFSET},
          nodeTable{nodeTable}, relTable{relTable}, lowerBound{lowerBound},
          upperBound{upperBound}, srcOffset{srcOffset}, context{context} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<ShortestPathAlgoBindData>(columnTypes, columnNames, nodeTable,
            relTable, lowerBound, upperBound, srcOffset, context);
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
static std::unique_ptr<TableFuncBindData> bindFunc(ClientContext* context,
    TableFuncBindInput* input) {
    if (input->inputs.size() != 5) {
        throw BinderException("Not enough parameters for Shortest Path function ...");
    }
    auto nodeTable = input->inputs[0].getValue<std::string>();
    auto relTable = input->inputs[1].getValue<std::string>();
    auto lowerBound = input->inputs[2].getValue<int64_t>();
    auto upperBound = input->inputs[3].getValue<int64_t>();
    auto srcOffset = input->inputs[4].getValue<int64_t>();
    std::vector<LogicalType> columnTypes;
    std::vector<std::string> columnNames;
    columnTypes.push_back(*LogicalType::UINT64());
    columnTypes.push_back(*LogicalType::UINT64());
    columnNames.push_back("dst_offset");
    columnNames.push_back("path_length");
    return std::make_unique<ShortestPathAlgoBindData>(std::move(columnTypes),
        std::move(columnNames), nodeTable, relTable, lowerBound, upperBound, srcOffset, context);
}

struct ShortestPathAlgoSharedState : public CallFuncSharedState {
    std::unique_ptr<Graph> graph;
    common::offset_t srcOffset;
    std::unique_ptr<IFEMorsel> ifeMorsel;

    ShortestPathAlgoSharedState(common::offset_t maxOffset, common::offset_t curOffset,
        uint64_t morselSize, common::offset_t srcOffset, std::unique_ptr<IFEMorsel> ifeMorsel,
        std::unique_ptr<Graph> graph)
        : CallFuncSharedState{maxOffset, curOffset, morselSize}, graph{std::move(graph)},
          srcOffset{srcOffset}, ifeMorsel{std::move(ifeMorsel)} {}
};

std::unique_ptr<TableFuncSharedState> shortestPathAlgoInitSharedState(
    TableFunctionInitInput& input) {
    auto bindData = ku_dynamic_cast<TableFuncBindData*, ShortestPathAlgoBindData*>(input.bindData);
    auto graph =
        std::make_unique<OnDiskGraph>(bindData->context, bindData->nodeTable, bindData->relTable);
    auto maxOffset = graph->getNumNodes();
    auto ifeMorsel =
        std::make_unique<IFEMorsel>(bindData->upperBound, bindData->lowerBound, maxOffset);
    return std::make_unique<ShortestPathAlgoSharedState>(maxOffset, 0LU /* curOffset */,
        64LU /* hard-coding a morsel size for now*/, bindData->srcOffset, std::move(ifeMorsel),
        std::move(graph));
}

/*
 * This will be main function for logic, doing frontier extension and updating state.
 */
static common::offset_t extendFrontierFunc(TableFuncInput& input, TableFuncOutput& /*output*/) {
    auto sharedState =
        ku_dynamic_cast<TableFuncSharedState*, ShortestPathAlgoSharedState*>(input.sharedState);
    auto& graph = sharedState->graph;
    auto& ifeMorsel = sharedState->ifeMorsel;
    auto morsel = ifeMorsel->getMorsel(sharedState->morselSize);
    uint64_t numDstVisitedLocal = 0u;
    while (!ifeMorsel->isCompleteNoLock() && morsel.hasMoreToOutput()) {
        for (auto i = morsel.startOffset; i < morsel.endOffset; i++) {
            auto frontierOffset = ifeMorsel->bfsLevelNodeOffsets[i];
            auto dstNodeIDVector = sharedState->graph->getFwdNbrs(frontierOffset);
            for (auto j = 0u; j < dstNodeIDVector->state->selVector->selectedSize; j++) {
                auto pos = dstNodeIDVector->state->selVector->selectedPositions[j];
                auto dstNodeID = dstNodeIDVector->getValue<common::nodeID_t>(pos);
                auto state =
                    ifeMorsel->visitedNodes[dstNodeID.offset].load(std::memory_order_acq_rel);
                if (state == NOT_VISITED_DST) {
                    auto tryCAS = ifeMorsel->visitedNodes[dstNodeID.offset].compare_exchange_strong(
                        state, VISITED_DST_NEW, std::memory_order_acq_rel);
                    if (tryCAS) {
                        numDstVisitedLocal++;
                        ifeMorsel->pathLength[dstNodeID.offset].store(ifeMorsel->currentLevel + 1,
                            std::memory_order_relaxed);
                    }
                } else if (state == NOT_VISITED) {
                    ifeMorsel->visitedNodes[dstNodeID.offset].store(VISITED_NEW,
                        std::memory_order_acq_rel);
                }
            }
        }
        ifeMorsel->mergeResults(numDstVisitedLocal);
        morsel = ifeMorsel->getMorsel(sharedState->morselSize);
    }
    return 0;
}

static common::offset_t shortestPathOutputFunc(TableFuncInput& input, TableFuncOutput& output) {
    auto sharedState =
        ku_dynamic_cast<TableFuncSharedState*, ShortestPathAlgoSharedState*>(input.sharedState);
    auto& ifeMorsel = sharedState->ifeMorsel;
    auto morselSize = DEFAULT_VECTOR_CAPACITY;
    auto morsel = ifeMorsel->getDstWriteMorsel(morselSize);
    if (!morsel.hasMoreToOutput()) {
        return 0;
    }
    auto dstOffsetVector = output.dataChunk.valueVectors[0];
    auto pathLengthVector = output.dataChunk.valueVectors[1];
    auto pos = 0;
    for (auto offset = morsel.startOffset; offset < morsel.endOffset; offset++) {
        auto state = ifeMorsel->visitedNodes[offset].load(std::memory_order_acq_rel);
        uint64_t pathLength = ifeMorsel->pathLength[offset].load(std::memory_order_acq_rel);
        if ((state == VISITED_DST_NEW || state == VISITED_DST) &&
            pathLength >= ifeMorsel->lowerBound) {
            dstOffsetVector->setValue<uint64_t>(pos, offset);
            pathLengthVector->setValue<uint64_t>(pos, pathLength);
            pos++;
        }
    }
    if (pos == 0) {
        return shortestPathOutputFunc(input, output);
    }
    dstOffsetVector->state->selVector->selectedSize = pos;
    return pos;
}

ShortestPathAlgoSharedState* ShortestPath::getSharedState(Sink* sink) {
    auto algorithmRunner = ku_dynamic_cast<Sink*, AlgorithmRunner*>(sink);
    auto sharedState = algorithmRunner->getFuncSharedState();
    return ku_dynamic_cast<TableFuncSharedState*, ShortestPathAlgoSharedState*>(sharedState);
}

void ShortestPath::incrementTableFuncIdx(kuzu::processor::Sink* sink) {
    auto algorithmRunner = ku_dynamic_cast<Sink*, AlgorithmRunner*>(sink);
    algorithmRunner->incrementTableFuncIdx();
}

bool ShortestPath::compute(Sink* sink, ExecutionContext* executionContext,
    std::shared_ptr<ParallelUtils> parallelUtils) {
    auto shortestPathSharedState = getSharedState(sink);
    auto ifeMorsel = shortestPathSharedState->ifeMorsel.get();
    ifeMorsel->initSourceNoLock(shortestPathSharedState->srcOffset);
    while (!ifeMorsel->isCompleteNoLock()) {
        parallelUtils->doParallel(sink, executionContext);
        ifeMorsel->initializeNextFrontierNoLock();
    }
    incrementTableFuncIdx(sink);
    parallelUtils->doParallel(sink, executionContext);
}

function::function_set ShortestPath::getFunctionSet() {
    function_set functionSet;
    auto functionList = std::vector<table_func_t>({extendFrontierFunc, shortestPathOutputFunc});
    auto function = std::make_unique<TableFunction>(name, functionList, bindFunc,
        shortestPathAlgoInitSharedState, CallFunction::initEmptyLocalState,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING,
            LogicalTypeID::INT64, LogicalTypeID::INT64, LogicalTypeID::INT64});
    functionSet.push_back(std::move(function));
    return functionSet;
}

} // namespace graph
} // namespace kuzu
