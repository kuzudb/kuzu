#include "binder/binder.h"
#include "binder/expression/node_expression.h"
#include "common/exception/runtime.h"
#include "common/string_utils.h"
#include "function/algo_function.h"
#include "function/config/betweenness_centrality_config.h"
#include "function/config/max_iterations_config.h"
#include "function/gds/gds.h"
#include "function/gds/gds_utils.h"
#include "function/gds/gds_vertex_compute.h"
#include "function/table/table_function.h"
#include "processor/execution_context.h"
#include "storage/buffer_manager/memory_manager.h"

using namespace kuzu::processor;
using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::function;

namespace kuzu {
namespace algo_extension {

/**CONFIG**/

struct BetweennessCentralityOptionalParams final : public MaxIterationOptionalParams {
    OptionalParam<Direction> direction;
    OptionalParam<WeightProperty> weightProperty;

    explicit BetweennessCentralityOptionalParams(const expression_vector& optionalParams);

    BetweennessCentralityOptionalParams(OptionalParam<MaxIterations> maxIterations, OptionalParam<Direction> direction, OptionalParam<WeightProperty> weightProperty) 
        : MaxIterationOptionalParams{maxIterations}, direction{std::move(direction)}, weightProperty{std::move(weightProperty)}{}

    void evaluateParams(main::ClientContext* context) override {
        MaxIterationOptionalParams::evaluateParams(context);
        direction.evaluateParam(context);
        weightProperty.evaluateParam(context);
    }

    std::unique_ptr<function::OptionalParams> copy() override {
        return std::make_unique<BetweennessCentralityOptionalParams>(maxIterations, direction, weightProperty);
    }
};

BetweennessCentralityOptionalParams::BetweennessCentralityOptionalParams(const expression_vector& optionalParams)
    : MaxIterationOptionalParams{constructMaxIterationParam(optionalParams)}
{
    for (auto& optionalParam : optionalParams) {
        auto paramName = StringUtils::getLower(optionalParam->getAlias());
        if (paramName == WeightProperty::NAME) {
            weightProperty = function::OptionalParam<WeightProperty>(optionalParam);
        } else if (paramName == Direction::NAME) {
            direction = function::OptionalParam<Direction>(optionalParam);
        } else {
            throw RuntimeException {
                stringFormat("Unknown optional argument: {}", optionalParam->getAlias())};
        }
    }
}

struct BetweennessCentralityBindData final : public GDSBindData {
    BetweennessCentralityBindData(expression_vector columns, graph::NativeGraphEntry graphEntry,
        std::shared_ptr<Expression> nodeOutput,
        std::unique_ptr<BetweennessCentralityOptionalParams> optionalParams)
        : GDSBindData{std::move(columns), std::move(graphEntry), std::move(nodeOutput)} {
        this->optionalParams = std::move(optionalParams);
    }

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<BetweennessCentralityBindData>(*this);
    }
};

/**COMPUTE**/

struct BetweennessCentralityState {
    explicit BetweennessCentralityState(storage::MemoryManager* mm, const offset_t origNumNodes) : bc{mm, origNumNodes}, origNumNodes{origNumNodes}, adj{origNumNodes} {}
    ku_vector_t<std::atomic<double>> bc;
    const offset_t origNumNodes;
    // TODO(Tanvir): We should not be materializing the graph like this. Remove.
    std::vector<std::vector<offset_t>> adj;
};

class BrandesBCVertexCompute : public GDSVertexCompute {
public:

    BrandesBCVertexCompute(common::NodeOffsetMaskMap* nodeMask, storage::MemoryManager* mm, BetweennessCentralityState& state) : 
        GDSVertexCompute(nodeMask),
        state{state},
        distance{mm, state.origNumNodes},
        sigma{mm, state.origNumNodes},
        delta{mm, state.origNumNodes},
        visitOrder{mm},
        parents{state.origNumNodes},
        mm{mm}
    {}

    void beginOnTableInternal(table_id_t) override {}

    void vertexCompute(offset_t startOffset, offset_t endOffset, table_id_t) override {
      for (auto s = startOffset; s < endOffset; ++s) {
        // When doing approx if (skip[s] continue)
        initializeSSSP(s);
        runBFS(s);
        runBackprop(s);
      }
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<BrandesBCVertexCompute>(nodeMask, mm, state);
    }

private:
    void initializeSSSP(offset_t s) {
        std::fill(distance.begin(), distance.end(), INF);
        std::fill(sigma.begin(), sigma.end(), 0);
        std::fill(delta.begin(), delta.end(), 0);
        visitOrder.clear();
        for (auto& pvec : parents) {
            pvec.clear();
        }
        distance[s] = 0;
        sigma[s] = 1;
    }

    //TODO(Tanvir): (Use frontier Abstraction, modify for weighted graph)
    void runBFS(offset_t s) {
        ku_vector_t<offset_t> queue{mm};
        queue.push_back(s);
        for(auto i = 0u; i < queue.size(); ++i) {
            const auto cur = queue[i];
            visitOrder.push_back(cur);
            for(const auto& neighbour : state.adj[cur]) {
                if (distance[neighbour] == INF) {
                    distance[neighbour] = distance[cur]+1;
                    queue.push_back(neighbour);
                }
                if (distance[neighbour] == distance[cur]+1) {
                    sigma[neighbour]+=sigma[cur];
                    parents[neighbour].push_back(cur);
                }
            }
        }
    }

    void runBackprop(offset_t s) {
        while(!visitOrder.empty()) {
            const auto cur = visitOrder.back();
            visitOrder.pop_back();
            for(const auto& parent : parents[cur]) {
                delta[parent] += (1 + delta[cur]) * (double)((double)sigma[parent] / (double)sigma[cur]);
            }
            if (cur != s) {
                state.bc[cur].fetch_add(delta[cur], std::memory_order_relaxed);
            }
        }
    }
    static constexpr double INF = std::numeric_limits<double>::max();
    // Holds betweenness centrality scores for the graph.
    BetweennessCentralityState& state;
    // Helper container used in SSSP.
    ku_vector_t<double> distance;
    // sigma[v] <- Number of shortest paths from source vertex to v.
    ku_vector_t<uint64_t> sigma; 
    // Dependency scores <- We compute this during back propagation and update
    // BC scores.
    ku_vector_t<double> delta;
    // Stack used in back propagation step. We populate when computing SSSP.
    ku_vector_t<offset_t> visitOrder;
    // TODO(Tanvir): We cannot use std::vector to store parent lists. 
    // Use kuzu_vector<ParentList*> ?
    std::vector<std::vector<offset_t>> parents;
    // Only stored for copy method.
    storage::MemoryManager* mm;
};

/**RESULTS**/

class WriteResultsBC : public GDSResultVertexCompute
{
public:
    WriteResultsBC(storage::MemoryManager* mm, GDSFuncSharedState* sharedState, const ku_vector_t<std::atomic<double>>& finalResults)
        : GDSResultVertexCompute{mm, sharedState}, finalResults{finalResults} {
        nodeIDVector = createVector(LogicalType::INTERNAL_ID());
        scoreVector = createVector(LogicalType::DOUBLE());
    }

    void beginOnTableInternal(table_id_t /*tableID*/) override {}

    void vertexCompute(const offset_t startOffset, const offset_t endOffset,
        const table_id_t tableID) override {
        for (auto i = startOffset; i < endOffset; ++i) {
            const auto nodeID = nodeID_t{i, tableID};
            nodeIDVector->setValue<nodeID_t>(0, nodeID);
            scoreVector->setValue<double>(0, finalResults[i].load());
            localFT->append(vectors);
        }
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<WriteResultsBC>(mm, sharedState, finalResults);
    }

private:
    const ku_vector_t<std::atomic<double>>& finalResults;
    std::unique_ptr<ValueVector> nodeIDVector;
    std::unique_ptr<ValueVector> scoreVector;
};

/**GDS SETUP**/

static common::offset_t tableFunc(const TableFuncInput& input, TableFuncOutput&) {
    const auto clientContext = input.context->clientContext;
    auto sharedState = input.sharedState->ptrCast<GDSFuncSharedState>();
    auto graph = sharedState->graph.get();
    KU_ASSERT(graph->getNodeTableIDs().size() == 1);
    const auto tableId = graph->getNodeTableIDs()[0];
    auto mm = clientContext->getMemoryManager();
    const auto nbrTables = graph->getRelInfos(tableId);
    const auto nbrInfo = nbrTables[0];
    KU_ASSERT(nbrInfo.srcTableID == nbrInfo.dstTableID);

    // TODO(Tanvir) Use config arguments to modify traversal and SSSP.
    // Currently hard coded to work with a directed unweighted graph.
    auto betweennessCentralityBindData = input.bindData->constPtrCast<BetweennessCentralityBindData>();
    auto& config = betweennessCentralityBindData->optionalParams->constCast<BetweennessCentralityOptionalParams>();
    std::vector<std::string> relProps;
    if (!config.weightProperty.getParamVal().empty()) {
        relProps.push_back(config.weightProperty.getParamVal());
    }
    const auto scanState = graph->prepareRelScan(*nbrInfo.relGroupEntry, nbrInfo.relTableID, nbrInfo.dstTableID, {});
    const auto origNumNodes = graph->getMaxOffset(clientContext->getTransaction(), tableId);

    // 1. Init state
    BetweennessCentralityState state{mm, origNumNodes};
    // 2. TODO(Use abstractions to avoid this) Materialize Graph
    for (auto nodeId = 0u; nodeId < origNumNodes; ++nodeId) {
        const nodeID_t nextNodeId = {nodeId, tableId};
        for (auto chunk : graph->scanFwd(nextNodeId, *scanState)) {
            chunk.forEach([&](auto neighbors, auto, auto i) {
                auto nbrId = neighbors[i].offset;
                if (nbrId == nodeId) {
                    // Ignore self-loops.
                    return;
                }
                state.adj[nodeId].push_back(nbrId);
            });
        }
    }
    // 3. Compute
    BrandesBCVertexCompute brandesBC(sharedState->getGraphNodeMaskMap(), mm, state);
    GDSUtils::runVertexCompute(input.context, GDSDensityState::DENSE, sharedState->graph.get(), brandesBC);
    // 4. Write Results
    const auto vertexCompute = std::make_unique<WriteResultsBC>(mm, sharedState, state.bc);
    GDSUtils::runVertexCompute(input.context, GDSDensityState::DENSE, graph, *vertexCompute);
    sharedState->factorizedTablePool.mergeLocalTables();

    return 0;
}

static constexpr char BETWEENNESS_CENTRALITY_SCORE[] = "betweenness_centrality_score";

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    auto graphName = input->getLiteralVal<std::string>(0);
    auto graphEntry = GDSFunction::bindGraphEntry(*context, graphName);
    auto nodeOutput = GDSFunction::bindNodeOutput(*input, graphEntry.getNodeEntries());
    expression_vector columns;
    columns.push_back(nodeOutput->constCast<NodeExpression>().getInternalID());
    columns.push_back(input->binder->createVariable(BETWEENNESS_CENTRALITY_SCORE, LogicalType::DOUBLE()));
    return std::make_unique<BetweennessCentralityBindData>(std::move(columns), std::move(graphEntry), nodeOutput,
        std::make_unique<BetweennessCentralityOptionalParams>(input->optionalParamsLegacy));
}

function_set BetweennessCentrality::getFunctionSet() {
    function_set result;
    auto func = std::make_unique<TableFunction>(BetweennessCentrality::name,
        std::vector<LogicalTypeID>{LogicalTypeID::ANY});
    func->bindFunc = bindFunc;
    func->tableFunc = tableFunc;
    func->initSharedStateFunc = GDSFunction::initSharedState;
    func->initLocalStateFunc = TableFunction::initEmptyLocalState;
    func->canParallelFunc = [] { return false; };
    func->getLogicalPlanFunc = GDSFunction::getLogicalPlan;
    func->getPhysicalPlanFunc = GDSFunction::getPhysicalPlan;
    result.push_back(std::move(func));
    return result;
}

}
}
