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

struct BetweennessCentralityState
{
    explicit BetweennessCentralityState(storage::MemoryManager* mm, const offset_t origNumNodes) : bc{mm, origNumNodes}, adj{origNumNodes} {}
    ku_vector_t<double> bc;
    std::vector<std::vector<offset_t>> adj;
};

static void bfs
    (
    const BetweennessCentralityState& state, 
    const offset_t srcId, 
    std::vector<uint64_t>& sigma, 
    std::vector<offset_t>& visitOrder,
    std::vector<std::vector<offset_t>>& parents
    )
{
    std::vector<uint64_t> distance(parents.size(), UINT64_MAX);
    distance[srcId] = 0;
    sigma[srcId] = 1;
    std::vector<int> q;
    q.push_back(srcId);
    for(auto i = 0u; i < q.size(); ++i) {
        auto cur = q[i];
        visitOrder.push_back(cur);
        for(const auto& neighbour : state.adj[cur]) {
            if (distance[neighbour] == UINT64_MAX) {
                distance[neighbour] = distance[cur]+1;
                q.push_back(neighbour);
            }
            if (distance[neighbour] == distance[cur]+1) {
                sigma[neighbour]+=sigma[cur];
                parents[neighbour].push_back(cur);
            }
        }
    }
}

static void backProp
    (
    BetweennessCentralityState& state, 
    const offset_t srcId, 
    std::vector<uint64_t>& sigma, 
    std::vector<offset_t>& visitOrder,
    std::vector<std::vector<offset_t>>& parents
    )
{
    std::vector<double> delta(state.bc.size());
    while(!visitOrder.empty()) {
        auto cur = visitOrder.back(); visitOrder.pop_back();
        for(const auto& parent : parents[cur]) {
            delta[parent] += (1 + delta[cur]) * (double)((double)sigma[parent] / (double)sigma[cur]);
        }
        if (cur != srcId) {
            state.bc[cur]+=delta[cur];
        }
    }
}

/**RESULTS**/

class WriteResultsBC : public GDSResultVertexCompute
{
public:
    WriteResultsBC(storage::MemoryManager* mm, GDSFuncSharedState* sharedState, const ku_vector_t<double>& finalResults)
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
            scoreVector->setValue<double>(0, finalResults[i]);
            localFT->append(vectors);
        }
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<WriteResultsBC>(mm, sharedState, finalResults);
    }

private:
    const ku_vector_t<double>& finalResults;
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
    //auto betweennessCentralityBindData = input.bindData->constPtrCast<BetweennessCentralityBindData>();
    //auto& config = betweennessCentralityBindData->optionalParams->constCast<BetweennessCentralityOptionalParams>();
    const auto scanState = graph->prepareRelScan(*nbrInfo.relGroupEntry, nbrInfo.relTableID, nbrInfo.dstTableID, {});
    const auto origNumNodes = graph->getMaxOffset(clientContext->getTransaction(), tableId);

    // Single Threaded
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

    // 3. For each (v in V):
    //    a. Run BFS
    //    b. Run backprop
    for (auto nodeId = 0u; nodeId < origNumNodes; ++nodeId) {
        std::vector<uint64_t> sigma(origNumNodes, 0);
        std::vector<offset_t> visitOrder;
        std::vector<std::vector<offset_t>> parents(origNumNodes);
        bfs(state, nodeId, sigma, visitOrder, parents);
        backProp(state, nodeId, sigma, visitOrder, parents);
    }
    // 4. Write Results
    const auto vertexCompute = std::make_unique<WriteResultsBC>(mm, sharedState, state.bc);
    GDSUtils::runVertexCompute(input.context, GDSDensityState::DENSE, graph, *vertexCompute);
    sharedState->factorizedTablePool.mergeLocalTables();

    return 0;
}

static constexpr char BETWEENNESS_CENTRALITY_SCORE[] = "betweeness_centrality_score";

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
