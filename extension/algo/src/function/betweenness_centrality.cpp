#include "binder/binder.h"
#include "binder/expression/node_expression.h"
#include "common/exception/runtime.h"
#include "common/string_utils.h"
#include "common/types/types.h"
#include "function/algo_function.h"
#include "function/config/betweenness_centrality_config.h"
#include "function/config/max_iterations_config.h"
#include "function/gds/auxiliary_state/gds_auxilary_state.h"
#include "function/gds/gds.h"
#include "function/gds/gds_utils.h"
#include "function/gds/gds_vertex_compute.h"
#include "function/table/bind_input.h"
#include "function/table/table_function.h"
#include "graph/graph.h"
#include "main/client_context.h"
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

    BetweennessCentralityOptionalParams(OptionalParam<MaxIterations> maxIterations,
        OptionalParam<Direction> direction, OptionalParam<WeightProperty> weightProperty)
        : MaxIterationOptionalParams{maxIterations}, direction{std::move(direction)},
          weightProperty{std::move(weightProperty)} {}

    void evaluateParams(main::ClientContext* context) override {
        MaxIterationOptionalParams::evaluateParams(context);
        direction.evaluateParam(context);
        weightProperty.evaluateParam(context);
    }

    std::unique_ptr<function::OptionalParams> copy() override {
        return std::make_unique<BetweennessCentralityOptionalParams>(maxIterations, direction,
            weightProperty);
    }
};

BetweennessCentralityOptionalParams::BetweennessCentralityOptionalParams(
    const expression_vector& optionalParams)
    : MaxIterationOptionalParams{constructMaxIterationParam(optionalParams)} {
    for (auto& optionalParam : optionalParams) {
        auto paramName = StringUtils::getLower(optionalParam->getAlias());
        if (paramName == WeightProperty::NAME) {
            weightProperty = function::OptionalParam<WeightProperty>(optionalParam);
        } else if (paramName == Direction::NAME) {
            direction = function::OptionalParam<Direction>(optionalParam);
        } else {
            throw RuntimeException{
                stringFormat("Unknown optional argument: {}", optionalParam->getAlias())};
        }
    }
}

struct BetweennessCentralityBindData final : public GDSBindData {
    BetweennessCentralityBindData(expression_vector columns, graph::NativeGraphEntry graphEntry,
        std::shared_ptr<Expression> nodeOutput,
        std::unique_ptr<BetweennessCentralityOptionalParams> optionalParams)
        : GDSBindData{std::move(columns), std::move(graphEntry), expression_vector{nodeOutput}} {
        this->optionalParams = std::move(optionalParams);
    }

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<BetweennessCentralityBindData>(*this);
    }
};

/**STATES**/

// Stores our final results. Each iteration of Brandes' algorithm will partially
// update the betweennessCentrality container.
struct BetweennessCentralityState {
    BetweennessCentralityState(storage::MemoryManager* mm, const offset_t numNodes)
        : betweennessCentrality{mm, static_cast<size_t>(numNodes)}, numNodes{numNodes} {}
    ku_vector_t<std::atomic<double>> betweennessCentrality;
    const offset_t numNodes;
};

// Stores information to populate when preforming a forward traversal.
struct BCFwdData {
    // pathScore is weight of the path from the source.
    struct PathData {
        std::atomic<uint64_t> numPaths = 0;
        std::atomic<double> pathScore = INF;
        static constexpr double INF = std::numeric_limits<double>::max();
    };
    // LevelData stores the iteration at which node was found.
    // After the forward traversal is complete, a backwards traversal must be
    // done from the furthest nodes (nodes with the greatest level).
    struct LevelData {
        uint64_t level = 0;
        offset_t node = 0;
        auto operator<=>(const LevelData& rhs) const { return level <=> rhs.level; }
    };

    BCFwdData(storage::MemoryManager* mm, const offset_t numNodes)
        : nodePathData{mm, static_cast<size_t>(numNodes)},
          levels{mm, static_cast<size_t>(numNodes)} {}

    void init(const offset_t sourceNode) {
        // The fill parts may be done in parallel.
        for (auto& path : nodePathData) {
            path.numPaths.store(PathData{}.numPaths);
            path.pathScore.store(PathData{}.pathScore);
        }
        for(auto i = 0u; i < levels.size(); ++i) { levels[i] = LevelData{.level=LevelData{}.level, .node = i}; }
        nodePathData[sourceNode].numPaths.store(1);
        nodePathData[sourceNode].pathScore.store(0);
    }
    // Collection of PathData and LevelData indexed by vertex.
    ku_vector_t<PathData> nodePathData;
    ku_vector_t<LevelData> levels;
};

// This should be our traversal wrapper. This wrapper *should* work as an
// interface. TODO(Tanvir) implement different queues for weighted and
// unweighted graphs. The unweighted graph does not require a priority queue.
class BCFwdTraverse {
public:
    explicit BCFwdTraverse(storage::MemoryManager* mm) : heap(mm) {}

    void init(const offset_t sourceNode) {
        heap.clear();
        push({0, sourceNode});
    }

    void push(const std::pair<double, offset_t>& val) {
        heap.push_back(val);
        std::push_heap(heap.begin(), heap.end(), comp);
    }

    std::pair<double, offset_t>& top() { return heap.front(); }

    void pop() {
        std::pop_heap(heap.begin(), heap.end(), comp);
        heap.pop_back();
    }

    bool empty() const { return heap.empty(); }

private:
    ku_vector_t<std::pair<double, offset_t>> heap;
    std::greater<std::pair<double, offset_t>> comp;
};

// dependencyScores accumulate during backwards traversal over the graph. Once
// traversal is complete these scores should accumulate into the betweenness centrality scores.
struct BCBwdData {
    BCBwdData(storage::MemoryManager* mm, const offset_t numNodes)
        : dependencyScores{mm, static_cast<size_t>(numNodes)} {}

    void init() {
        // This can be done in parallel.
        std::fill(dependencyScores.begin(), dependencyScores.end(), 0);
    }

    ku_vector_t<double> dependencyScores;
};

/**Compute**/

class UnweightedFwdTraverse : public EdgeCompute {
public:
    explicit UnweightedFwdTraverse(BCFwdData& fwdData) : fwdData{fwdData} {}

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, graph::NbrScanState::Chunk& chunk,
        bool) override {
        auto curDistance = fwdData.nodePathData[boundNodeID.offset].pathScore.load();
        auto curPaths = fwdData.nodePathData[boundNodeID.offset].numPaths.load();
        std::vector<nodeID_t> result;
        chunk.forEach([&](auto neighbors, auto, auto i) {
            auto nbrNodeID = neighbors[i];
            auto nbrDistance = BCFwdData::PathData::INF;
            // We see the node for the first time.
            if (fwdData.nodePathData[nbrNodeID.offset].pathScore.compare_exchange_strong(
                    nbrDistance, curDistance + 1)) {
                // init level.
                fwdData.levels[nbrNodeID.offset].level =
                    fwdData.levels[boundNodeID.offset].level + 1;
                fwdData.nodePathData[nbrNodeID.offset].numPaths.fetch_add(curPaths,
                    std::memory_order_relaxed);
                result.push_back(nbrNodeID);
            } 
            // Reaching here means CAS failed, so nbrDistance is correct.
            else if (nbrDistance == curDistance+1) {
                fwdData.nodePathData[nbrNodeID.offset].numPaths.fetch_add(curPaths, std::memory_order_relaxed);
            }
        });
        return result;
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<UnweightedFwdTraverse>(fwdData);
    }

private:
    BCFwdData& fwdData;
};

// static void SSSPChunkCompute(const graph::NbrScanState::Chunk& chunk, BCFwdData& fwdData,
//                              BCFwdTraverse& fwdTraverse, const offset_t& cur, const double&
//                              curWeight) {
//     static constexpr double DEFAULT_WEIGHT = 1;
//     (void)DEFAULT_WEIGHT;
//     chunk.forEach([&](auto neighbors, auto propertyVectors, auto i) {
//         auto nbrId = neighbors[i].offset;
//         if (nbrId == cur) {
//             // Ignore self-loops.
//             return;
//         }
//         const auto weight =
//             (propertyVectors.empty() ? DEFAULT_WEIGHT :
//             propertyVectors.front()->template getValue<double>(i));
//         if (weight < 0) {
//             throw common::RuntimeException(common::stringFormat(
//                 "Found negative weight {}. This is not supported by Betweenness Centrality.",
//                 weight));
//         }
//         // Check for a better path weight (all weights are initially *infinite*).
//         // If so the number of shortest paths already accumulated must reset.
//         // The level must also be updated accordingly.
//         if (fwdData.nodePathData[nbrId].pathScore > curWeight + weight) {
//             fwdData.nodePathData[nbrId].pathScore = curWeight + weight;
//             fwdData.nodePathData[nbrId].numPaths = 0;
//             fwdData.levels[nbrId] = {fwdData.levels[cur].level + 1, nbrId};
//             fwdTraverse.push({curWeight + weight, nbrId});
//         }
//         // A path has been found matching our best path weight (potentially the first path).
//         // The number of shortestPaths can be incremented by the number of ways to reach the
//         parent. if (fwdData.nodePathData[nbrId].pathScore == fwdData.nodePathData[cur].pathScore
//         + weight) {
//             fwdData.nodePathData[nbrId].numPaths += fwdData.nodePathData[cur].numPaths;
//         }
//     });
// }
//
//// TOOD(Tanvir): Currently always do the traversal for weighted graphs (treat unweighted
//// edges as weight 1). This should change (we don't always need a pq).
// static void SSSPCompute(BCFwdData& fwdData, BCFwdTraverse& fwdTraverse, graph::Graph* graph,
//     graph::NbrScanState* scanState, const table_id_t tableId, bool undirected) {
//     while (!fwdTraverse.empty()) {
//         const auto [curWeight, cur] = fwdTraverse.top();
//         fwdTraverse.pop();
//         // If the path to get to this vertex is worse than what has already been
//         // computed there is no further computation to do with this path.
//         if (curWeight > fwdData.nodePathData[cur].pathScore) {
//             continue;
//         }
//         // Whether a fwd or bwd scan is being done the logic for traversal remains the same.
//         const nodeID_t nextNodeId = {cur, tableId};
//         for (auto chunk : graph->scanFwd(nextNodeId, *scanState)) {
//             SSSPChunkCompute(chunk, fwdData, fwdTraverse, cur, curWeight);
//         }
//         if (!undirected) {
//             continue;
//         }
//         for (auto chunk : graph->scanBwd(nextNodeId, *scanState)) {
//             SSSPChunkCompute(chunk, fwdData, fwdTraverse, cur, curWeight);
//         }
//     }
// }

static void backwardsChunkCompute(const graph::NbrScanState::Chunk& chunk, BCFwdData& fwdData,
    BCBwdData& bwdData, const offset_t& cur) {
    static constexpr double DEFAULT_WEIGHT = 1;
    (void)DEFAULT_WEIGHT;
    chunk.forEach([&](auto neighbors, auto propertyVectors, auto i) {
        auto nbrId = neighbors[i].offset;
        if (nbrId == cur) {
            // Ignore self-loops.
            return;
        }
        const auto weight = propertyVectors.empty() ?
                                DEFAULT_WEIGHT :
                                propertyVectors.front()->template getValue<double>(i);
        // If the path weight to reach the neighbour + the edge weight from (or to) the
        // neighbour matches the weight of the best path to the current vertex
        // that neighbour is a parent the current vertex depends on.
        if (fwdData.nodePathData[nbrId].pathScore + weight == fwdData.nodePathData[cur].pathScore) {
            bwdData.dependencyScores[nbrId] +=
                (static_cast<double>(fwdData.nodePathData[nbrId].numPaths) /
                    fwdData.nodePathData[cur].numPaths) *
                (1 + bwdData.dependencyScores[cur]);
        }
    });
}

static void backwardsCompute(BCFwdData& fwdData, BCBwdData& bwdData,
    BetweennessCentralityState& state, graph::Graph* graph, graph::NbrScanState* scanState,
    const table_id_t tableId, const offset_t sourceNode) {
    // Sort so nodes are processed furthest from the source first (i.e higher levels first).
    std::sort(fwdData.levels.begin(), fwdData.levels.end(), std::greater<BCFwdData::LevelData>{});
    for (auto [lvl, cur] : fwdData.levels) {
        // If the node was not reached during the SSSP traversal or it is the
        // source node there is no computation to do.
        if (lvl == 0) {
            continue;
        }
        // Nothing to compute if we can't reach this node.
        if (fwdData.nodePathData[cur].numPaths == 0) {
            continue;
        }
        // Check for any dependencies; when found update dependency scores.
        const nodeID_t nextNodeId = {cur, tableId};
        for (auto chunk : graph->scanBwd(nextNodeId, *scanState)) {
            backwardsChunkCompute(chunk, fwdData, bwdData, cur);
        }
    }
    for (auto node = 0u; node < state.numNodes; ++node) {
        if (node != sourceNode) {
            state.betweennessCentrality[node].fetch_add(bwdData.dependencyScores[node]);
        }
    }
}

/**RESULTS**/

class WriteResultsBC : public GDSResultVertexCompute {
public:
    WriteResultsBC(storage::MemoryManager* mm, GDSFuncSharedState* sharedState,
        const ku_vector_t<std::atomic<double>>& finalResults, const bool undirected)
        : GDSResultVertexCompute{mm, sharedState}, finalResults{finalResults},
          undirected{undirected} {
        nodeIDVector = createVector(LogicalType::INTERNAL_ID());
        scoreVector = createVector(LogicalType::DOUBLE());
    }

    void beginOnTableInternal(table_id_t /*tableID*/) override {}

    void vertexCompute(const offset_t startOffset, const offset_t endOffset,
        const table_id_t tableID) override {
        for (auto i = startOffset; i < endOffset; ++i) {
            const auto nodeID = nodeID_t{i, tableID};
            nodeIDVector->setValue<nodeID_t>(0, nodeID);
            scoreVector->setValue<double>(0, finalResults[i].load() / (undirected ? 2 : 1));
            localFT->append(vectors);
        }
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<WriteResultsBC>(mm, sharedState, finalResults, undirected);
    }

private:
    const ku_vector_t<std::atomic<double>>& finalResults;
    const bool undirected;
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

    auto betweennessCentralityBindData =
        input.bindData->constPtrCast<BetweennessCentralityBindData>();
    auto& config = betweennessCentralityBindData->optionalParams
                       ->constCast<BetweennessCentralityOptionalParams>();

    if (!config.weightProperty.getParamVal().empty() &&
        !nbrInfo.relGroupEntry->containsProperty(config.weightProperty.getParamVal())) {
        throw RuntimeException{
            stringFormat("Cannot find property: {}", config.weightProperty.getParamVal())};
    }
    if (!config.weightProperty.getParamVal().empty() &&
        !LogicalTypeUtils::isNumerical(
            nbrInfo.relGroupEntry->getProperty(config.weightProperty.getParamVal()).getType())) {
        throw RuntimeException{stringFormat("Provided weight property is not numerical: {}",
            config.weightProperty.getParamVal())};
    }

    std::vector<std::string> relProps;
    if (!config.weightProperty.getParamVal().empty()) {
        relProps.push_back(config.weightProperty.getParamVal());
    }
    bool undirected =
        StringUtils::getLower(config.direction.getParamVal()) == Direction::UNDIRECTED;
    const auto scanState = graph->prepareRelScan(*nbrInfo.relGroupEntry, nbrInfo.relTableID,
        nbrInfo.dstTableID, relProps);
    const auto numNodes = graph->getMaxOffset(clientContext->getTransaction(), tableId);
    BetweennessCentralityState state{mm, numNodes};
    BCFwdData fwdData(mm, numNodes);
    BCBwdData bwdData(mm, numNodes);
    auto maxIterations = input.bindData->optionalParams->constCast<MaxIterationOptionalParams>().maxIterations.getParamVal();

    for (auto i = 0u; i < numNodes; ++i) {
        // Forward Traverse
        fwdData.init(i /*sourceNode*/);
        auto currentFrontier = DenseFrontier::getUnvisitedFrontier(input.context, graph);
        auto nextFrontier = DenseFrontier::getUnvisitedFrontier(input.context, graph);
        auto frontierPair = std::make_unique<DenseFrontierPair>(std::move(currentFrontier), std::move(nextFrontier));
        auto computeState = GDSComputeState(std::move(frontierPair), nullptr, nullptr);
        computeState.edgeCompute = std::make_unique<UnweightedFwdTraverse>(fwdData);
        computeState.auxiliaryState = std::make_unique<EmptyGDSAuxiliaryState>();
        computeState.frontierPair->resetCurrentIter();
        computeState.initSource({i, tableId});
        GDSUtils::runAlgorithmEdgeCompute(input.context, computeState, graph, ExtendDirection::FWD, maxIterations);
        // Backward Traverse
        bwdData.init();
        backwardsCompute(fwdData, bwdData, state, graph, scanState.get(), tableId, i /*sourceNode*/);
    }

    const auto vertexCompute = std::make_unique<WriteResultsBC>(mm, sharedState, state.betweennessCentrality, undirected);
    GDSUtils::runVertexCompute(input.context, GDSDensityState::DENSE, graph, *vertexCompute);
    sharedState->factorizedTablePool.mergeLocalTables();
    return 0;
}

static constexpr char BETWEENNESS_CENTRALITY_SCORE[] = "betweenness_centrality_score";

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    auto graphName = input->getLiteralVal<std::string>(0);
    auto graphEntry = GDSFunction::bindGraphEntry(*context, graphName);
    if (graphEntry.nodeInfos.size() != 1) {
        throw BinderException(std::string(BetweennessCentrality::name) +
                              " only supports operations on one node table.");
    }
    if (graphEntry.relInfos.size() != 1) {
        throw BinderException(std::string(BetweennessCentrality::name) +
                              " only supports operations on one rel table.");
    }
    auto nodeOutput = GDSFunction::bindNodeOutput(*input, graphEntry.getNodeEntries());
    expression_vector columns;
    columns.push_back(nodeOutput->constCast<NodeExpression>().getInternalID());
    columns.push_back(
        input->binder->createVariable(BETWEENNESS_CENTRALITY_SCORE, LogicalType::DOUBLE()));
    return std::make_unique<BetweennessCentralityBindData>(std::move(columns),
        std::move(graphEntry), nodeOutput,
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

} // namespace algo_extension
} // namespace kuzu
