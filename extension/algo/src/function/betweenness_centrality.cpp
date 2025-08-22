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

// Computes Betweenness Centrality on a graph G = (V, E)
// G may be directed or undirected. G may be weighted or unweighted.
// - https://snap.stanford.edu/class/cs224w-readings/brandes01centrality.pdf
// - https://en.wikipedia.org/wiki/Betweenness_centrality

// Betweenness centrality measures how often a node acts as a bridge 
// along the shortest paths between other nodes.

// The score for a vertex V is the sum of (sigma_st(V) / sigma_st) 
// over all distinct pairs of vertices S and T (S != T, S != V, T != V),
// where sigma_st is the number of shortest paths from S to T,
// and sigma_st(V) is the number of those paths that pass through V.

// The computation described in the explanation above is too expensive to run.
// Brandes' algorithm is what is implemented below.

// Pseudocode: Betweenness Centrality
// Input:  G = (V, E)
// Output: BC[v] = betweenness centrality scores for all v in V

// BC[v] : betweenness centrality score for vertex v (initialize to 0).
// For each source vertex s in V: <- Can do an approximation by taking a subset of V
//   - dist[w] : distance from s to w (initialize dist[s] = 0, others = INF)
//   - numSP[w] : number of shortest paths from s to w (initialize numSP[s] = 1, others = 0)
//   - level[w] : BFS level (hops) of w from s 
//                (for weighted graphs, take the maximum level over all shortest paths)
//   - dep[w] : dependency score for w (initialize all = 0)

//   - Run SSSP from s
//       - While traversing unweighted graph (BFS):
//           * When visiting a vertex p, examine each neighbor w:
//               - If w is discovered for the first time:
//                   * Set dist[w] = dist[p]+1
//                   * Set level[w] = dist[p]+1
//               - If dist[w] == dist[p] + 1 (shortest path via p):
//                   * Update numSP[w] += numSP[p]
//
//       - For weighted graphs, use two passes:
//           * First pass: run Bellman-Ford to compute all dist[w]
//             (as in Ligra: https://jshun.csail.mit.edu/ligra.pdf)
//           * Second pass: stabilize path counts
//               - For each vertex p, examine each neighbor w:
//                   If dist[p] + weight(p, w) == dist[w]:
//                     * Update level[w] = max(level[w], level[p] + 1)
//                     * Update numSP[w] += numSP[p]

//   - Sort vertices by decreasing level (farthest first)
//   - For each vertex u in that order (can do an iteration in parallel for vertices with matching levels):
//       * Identify parents p of u as neighbors with dist[p] + weight(p, u) == dist[u]
//       * For each parent p:
//             dep[p] += (numSP[p] / numSP[u]) * (1 + dep[u])
//       * If u != s, add dep[u] to BC[u]


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
    struct PathData {
        std::atomic<uint64_t> numSP = 0;
        std::atomic<double> dist = INF;
        static constexpr double INF = std::numeric_limits<double>::max();
    };
    struct LevelData {
        offset_t node = 0;
        std::atomic<uint64_t> level = 0;
        auto operator<=>(const LevelData& rhs) const { return level.load() <=> rhs.level.load(); }

        LevelData() = default;

        LevelData(const LevelData& other) : node(other.node), level(other.level.load()) {}

        LevelData& operator=(const LevelData& other) {
            if (this == &other) {
                return *this;
            }
            node = other.node;
            level.store(other.level.load());
            return *this;
        }
    };

    BCFwdData(storage::MemoryManager* mm, const offset_t numNodes)
        : nodePathData{mm, static_cast<size_t>(numNodes)},
          levels{mm, static_cast<size_t>(numNodes)} {}

    // TODO: Use vertex compute for this.
    void init(const offset_t sourceNode) {
        for (auto& path : nodePathData) {
            path.dist.store(PathData{}.dist);
            path.numSP.store(PathData{}.numSP);
        }
        for (auto i = 0u; i < levels.size(); ++i) {
            levels[i].node = i;
            levels[i].level.store(0);
        }
        nodePathData[sourceNode].numSP.store(1);
        nodePathData[sourceNode].dist.store(0);
    }

    ku_vector_t<PathData> nodePathData;
    ku_vector_t<LevelData> levels;
};

// dependencyScores accumulate during backwards traversal over the graph. Once
// traversal is complete these scores should accumulate into the betweenness centrality scores.
struct BCBwdData {
    BCBwdData(storage::MemoryManager* mm, const offset_t numNodes)
        : dependencyScores{mm, static_cast<size_t>(numNodes)} {}

    // TODO: Use vertex compute for this.
    void init() {
        for (auto& dep : dependencyScores) {
            dep.store(double{});
        }
    }

    ku_vector_t<std::atomic<double>> dependencyScores;
};

/**Compute**/

// TODO: Use function from https://github.com/kuzudb/kuzu/pull/5916
// Remove this function.
// NOLINTNEXTLINE
static void getValueAsDouble(const std::shared_ptr<common::ValueVector>& valueVector,
    double& weight, const offset_t i) {
    switch (valueVector->dataType.getPhysicalType()) {
    case PhysicalTypeID::INT64: {
        weight = static_cast<double>(valueVector->getValue<int64_t>(i));
        break;
    }
    case PhysicalTypeID::INT32: {
        weight = static_cast<double>(valueVector->getValue<int32_t>(i));
        break;
    }
    case PhysicalTypeID::INT16: {
        weight = static_cast<double>(valueVector->getValue<int16_t>(i));
        break;
    }
    case PhysicalTypeID::INT8: {
        weight = static_cast<double>(valueVector->getValue<int8_t>(i));
        break;
    }
    case PhysicalTypeID::UINT64: {
        weight = static_cast<double>(valueVector->getValue<uint64_t>(i));
        break;
    }
    case PhysicalTypeID::UINT32: {
        weight = static_cast<double>(valueVector->getValue<uint32_t>(i));
        break;
    }
    case PhysicalTypeID::UINT16: {
        weight = static_cast<double>(valueVector->getValue<uint16_t>(i));
        break;
    }
    case PhysicalTypeID::UINT8: {
        weight = static_cast<double>(valueVector->getValue<uint8_t>(i));
        break;
    }
    case PhysicalTypeID::INT128: {
        Int128_t::tryCast(valueVector->getValue<int128_t>(i), weight);
        break;
    }
    case PhysicalTypeID::DOUBLE: {
        weight = valueVector->getValue<double>(i);
        break;
    }
    case PhysicalTypeID::FLOAT: {
        weight = static_cast<double>(valueVector->getValue<float>(i));
        break;
    }
    default:
        KU_UNREACHABLE;
        break;
    }
}

// First pass for weighted graphs.
class ComputeSPWeightedFwdTraverse : public EdgeCompute {
public:
    explicit ComputeSPWeightedFwdTraverse(BCFwdData& fwdData) : fwdData{fwdData} {}

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, graph::NbrScanState::Chunk& chunk,
        bool) override {
        auto curDistance = fwdData.nodePathData[boundNodeID.offset].dist.load();
        std::vector<nodeID_t> result;
        chunk.forEach([&](auto neighbors, auto propertyVectors, auto i) {
            auto nbrNodeID = neighbors[i];
            auto nbrDistance = fwdData.nodePathData[nbrNodeID.offset].dist.load();
            double weight = 0;
            getValueAsDouble(propertyVectors[0], weight, i);
            if (weight <= 0) {
                throw RuntimeException(stringFormat(
                    "Betweenness Centrality does not work on non-positive weights. Got {}",
                    weight));
            }
            while (nbrDistance > curDistance + weight) {
                if (fwdData.nodePathData[nbrNodeID.offset].dist.compare_exchange_strong(
                        nbrDistance, curDistance + weight)) {
                    result.push_back(nbrNodeID);
                    return;
                }
            }
        });
        return result;
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<ComputeSPWeightedFwdTraverse>(fwdData);
    }

private:
    BCFwdData& fwdData;
};

// Second pass for weighted graphs.
class CountSPWeightedFwdTraverse : public EdgeCompute {
public:
    explicit CountSPWeightedFwdTraverse(BCFwdData& fwdData) : fwdData{fwdData} {}

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, graph::NbrScanState::Chunk& chunk,
        bool) override {
        auto curDistance = fwdData.nodePathData[boundNodeID.offset].dist.load();
        auto curNumPaths = fwdData.nodePathData[boundNodeID.offset].numSP.load();
        auto curLevel = fwdData.levels[boundNodeID.offset].level.load();
        std::vector<nodeID_t> result;
        chunk.forEach([&](auto neighbors, auto propertyVectors, auto i) {
            auto nbrNodeID = neighbors[i];
            auto nbrDistance = fwdData.nodePathData[nbrNodeID.offset].dist.load();
            auto nbrLevel = fwdData.levels[nbrNodeID.offset].level.load();
            double weight = 0;
            getValueAsDouble(propertyVectors[0], weight, i);
            KU_ASSERT_UNCONDITIONAL(weight > 0);
            // If a shortest path has been found update the level and number
            // of shortest paths to the neighbour.
            if (curDistance + weight == nbrDistance) {
                fwdData.nodePathData[nbrNodeID.offset].numSP.fetch_add(curNumPaths);
                while (!fwdData.levels[nbrNodeID.offset].level.compare_exchange_strong(nbrLevel,
                    std::max(nbrLevel, curLevel + 1))) {}
                result.push_back(nbrNodeID);
            }
        });
        return result;
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<CountSPWeightedFwdTraverse>(fwdData);
    }

private:
    BCFwdData& fwdData;
};

class UnweightedFwdTraverse : public EdgeCompute {
public:
    explicit UnweightedFwdTraverse(BCFwdData& fwdData) : fwdData{fwdData} {}

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, graph::NbrScanState::Chunk& chunk,
        bool) override {
        auto curDistance = fwdData.nodePathData[boundNodeID.offset].dist.load();
        auto curPaths = fwdData.nodePathData[boundNodeID.offset].numSP.load();
        auto curLevel = fwdData.levels[boundNodeID.offset].level.load();
        std::vector<nodeID_t> result;
        chunk.forEach([&](auto neighbors, auto, auto i) {
            auto nbrNodeID = neighbors[i];
            auto nbrPathDistance = BCFwdData::PathData::INF;

            // See the node for the first time.
            if (fwdData.nodePathData[nbrNodeID.offset].dist.compare_exchange_strong(
                    nbrPathDistance, curDistance + 1)) {
                fwdData.nodePathData[nbrNodeID.offset].numSP.fetch_add(curPaths);
                fwdData.levels[nbrNodeID.offset].level.store(curLevel + 1);
                result.push_back(nbrNodeID);
            }

            // Reaching here means CAS failed, so nbrPathDistance is up to date.
            else if (nbrPathDistance == curDistance + 1) {
                fwdData.nodePathData[nbrNodeID.offset].numSP.fetch_add(curPaths);
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

class BwdTraverse : public EdgeCompute {
public:
    explicit BwdTraverse(BCFwdData& fwdData, BCBwdData& bwdData)
        : fwdData{fwdData}, bwdData{bwdData} {}

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, graph::NbrScanState::Chunk& chunk,
        bool) override {
        auto curDistance = fwdData.nodePathData[boundNodeID.offset].dist.load();
        auto curPaths = fwdData.nodePathData[boundNodeID.offset].numSP.load();
        auto curScore = bwdData.dependencyScores[boundNodeID.offset].load();
        chunk.forEach([&](auto neighbors, auto propertyVectors, auto i) {
            auto nbrNodeID = neighbors[i];
            auto nbrDistance = fwdData.nodePathData[nbrNodeID.offset].dist.load();
            double weight = 1;
            if (!propertyVectors.empty()) {
                getValueAsDouble(propertyVectors[0], weight, i);
            }
            if (nbrDistance + weight == curDistance) {
                auto nbrPaths = fwdData.nodePathData[nbrNodeID.offset].numSP.load();
                auto scoreToAdd = ((double)nbrPaths / curPaths) * (1 + curScore);
                bwdData.dependencyScores[nbrNodeID.offset].fetch_add(scoreToAdd);
            }
        });
        // Always return empty. Populate the next frontier using a reverse sweep.
        return {};
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<BwdTraverse>(fwdData, bwdData);
    }

private:
    BCFwdData& fwdData;
    BCBwdData& bwdData;
};

// At the end of each iteration, accumulate all computed dependency scores.
class UpdateBC : public GDSVertexCompute {
public:
    UpdateBC(NodeOffsetMaskMap* nodeMask, BCBwdData& bwdData, BetweennessCentralityState& state,
        const offset_t ignore)
        : GDSVertexCompute{nodeMask}, bwdData{bwdData}, state{state}, ignore{ignore} {}

    void beginOnTableInternal(table_id_t) override {}

    void vertexCompute(offset_t startOffset, offset_t endOffset, table_id_t) override {
        for (auto i = startOffset; i < endOffset; ++i) {
            if (i == ignore) {
                continue;
            }
            state.betweennessCentrality[i].fetch_add(bwdData.dependencyScores[i].load());
        }
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<UpdateBC>(nodeMask, bwdData, state, ignore);
    }

private:
    BCBwdData& bwdData;
    BetweennessCentralityState& state;
    const offset_t ignore;
};

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
    auto mm = storage::MemoryManager::Get(*clientContext);
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
    bool undirected = StringUtils::getLower(config.direction.getParamVal()) == Direction::UNDIRECTED;
    const auto numNodes = graph->getMaxOffset(clientContext->getTransaction(), tableId);
    BetweennessCentralityState state{mm, numNodes};
    BCFwdData fwdData(mm, numNodes);
    BCBwdData bwdData(mm, numNodes);
    auto maxIterations = input.bindData->optionalParams->constCast<MaxIterationOptionalParams>().maxIterations.getParamVal();

    auto currentFrontier = DenseFrontier::getUnvisitedFrontier(input.context, graph);
    auto nextFrontier = DenseFrontier::getUnvisitedFrontier(input.context, graph);
    auto frontierPair = std::make_unique<DenseFrontierPair>(std::move(currentFrontier), std::move(nextFrontier));
    auto computeState = GDSComputeState(std::move(frontierPair), nullptr, std::make_unique<EmptyGDSAuxiliaryState>());

    for (auto i = 0u; i < numNodes; ++i) {
        // Forward Traverse
        fwdData.init(i /*sourceNode*/);
        // Reset frontier from last run (if any).
        computeState.frontierPair->resetCurrentIter();
        computeState.frontierPair->ptrCast<DenseFrontierPair>()->resetValue(input.context, graph, FRONTIER_UNVISITED);
        // Init frontier with source vertex.
        computeState.frontierPair->addNodeToNextFrontier(i);
        // Forward traversal is different depending on if the graph is weighted.
        if (relProps.empty()) {
            computeState.edgeCompute = std::make_unique<UnweightedFwdTraverse>(fwdData);
            computeState.frontierPair->setActiveNodesForNextIter();
            GDSUtils::runAlgorithmEdgeCompute(input.context, computeState, graph,
                undirected ? ExtendDirection::BOTH : ExtendDirection::FWD, maxIterations, relProps);
        } else {
            computeState.edgeCompute = std::make_unique<ComputeSPWeightedFwdTraverse>(fwdData);
            computeState.frontierPair->setActiveNodesForNextIter();
            GDSUtils::runAlgorithmEdgeCompute(input.context, computeState, graph,
                undirected ? ExtendDirection::BOTH : ExtendDirection::FWD, maxIterations, relProps);
            computeState.frontierPair->addNodeToNextFrontier(i);
            computeState.edgeCompute = std::make_unique<CountSPWeightedFwdTraverse>(fwdData);
            computeState.frontierPair->setActiveNodesForNextIter();
            GDSUtils::runAlgorithmEdgeCompute(input.context, computeState, graph,
                undirected ? ExtendDirection::BOTH : ExtendDirection::FWD, maxIterations, relProps);
        }
        // Backward Traverse
        bwdData.init();

        std::sort(fwdData.levels.begin(), fwdData.levels.end(), std::greater<>{});

        // Do a reverse sweep. After sorting the vertices by level
        // process them by placing them into frontiers grouped by level.
        auto maxLevel = fwdData.levels.front().level.load();
        for (auto j = 0u; j < fwdData.levels.size() && maxLevel > 0;) {
            while (j < fwdData.levels.size() && fwdData.levels[j].level.load() == maxLevel) {
                computeState.frontierPair->addNodeToNextFrontier(fwdData.levels[j].node);
                ++j;
            }
            computeState.edgeCompute = std::make_unique<BwdTraverse>(fwdData, bwdData);
            computeState.frontierPair->setActiveNodesForNextIter();
            GDSUtils::runAlgorithmEdgeCompute(input.context, computeState, graph,
                undirected ? ExtendDirection::BOTH : ExtendDirection::BWD, maxIterations, relProps);
            --maxLevel;
        }

        const auto vertexCompute = std::make_unique<UpdateBC>(sharedState->getGraphNodeMaskMap(), bwdData, state, i);
        GDSUtils::runVertexCompute(input.context, GDSDensityState::DENSE, graph, *vertexCompute);
    }
    const auto vertexCompute =
        std::make_unique<WriteResultsBC>(mm, sharedState, state.betweennessCentrality, undirected);
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
