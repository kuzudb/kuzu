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
    struct PathData {
        uint32_t numPaths = 0;
        uint32_t level = 0;
        double pathScore = INF;
        static constexpr double INF = std::numeric_limits<double>::max();
    };
    struct LevelData {
        offset_t node = 0;
        uint32_t level = 0;
        auto operator<=>(const LevelData& rhs) const { return level <=> rhs.level; }
    };

    BCFwdData(storage::MemoryManager* mm, const offset_t numNodes)
        : nodePathData{mm, static_cast<size_t>(numNodes)},
          levels{mm, static_cast<size_t>(numNodes)} {}

    void init(const offset_t sourceNode) {
        // The fill parts may be done in parallel.
        for (auto& path : nodePathData) {
            path.store(PathData{});
        }
        nodePathData[sourceNode].store(PathData{.numPaths = 1, .level = 0, .pathScore = 0});
    }
    // Collection of PathData and LevelData indexed by vertex.
    ku_vector_t<std::atomic<PathData>> nodePathData;
    ku_vector_t<LevelData> levels;
};

// dependencyScores accumulate during backwards traversal over the graph. Once
// traversal is complete these scores should accumulate into the betweenness centrality scores.
struct BCBwdData {
    BCBwdData(storage::MemoryManager* mm, const offset_t numNodes)
        : dependencyScores{mm, static_cast<size_t>(numNodes)} {}

    void init() {
        // This can be done in parallel.
        for (auto& dep : dependencyScores) {
            dep = double{};
        }
    }

    ku_vector_t<std::atomic<double>> dependencyScores;
};

/**Compute**/

class WeightedFwdTraverse : public EdgeCompute {
public:
    explicit WeightedFwdTraverse(BCFwdData& fwdData) : fwdData{fwdData} {}

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, graph::NbrScanState::Chunk& chunk,
        bool) override {
        auto curScore = fwdData.nodePathData[boundNodeID.offset].load().pathScore;
        auto curPaths = fwdData.nodePathData[boundNodeID.offset].load().numPaths;
        auto curLevel = fwdData.nodePathData[boundNodeID.offset].load().level;
        std::vector<nodeID_t> result;
        chunk.forEach([&](auto neighbors, auto propertyVectors, auto i) {
            auto nbrNodeID = neighbors[i];
            auto nbrPathData = fwdData.nodePathData[nbrNodeID.offset].load();
            const auto weight = propertyVectors[0]->template getValue<double>(i);
            if (weight <= 0) {
                throw RuntimeException(stringFormat(
                    "Betweenness Centrality does not work on non-positive weights. Got {}",
                    weight));
            }
            // We find a better path.
            while (nbrPathData.pathScore > curScore + weight) {
                if (fwdData.nodePathData[nbrNodeID.offset].compare_exchange_strong(nbrPathData,
                        BCFwdData::PathData{.numPaths = curPaths,
                            .level = curLevel + 1,
                            .pathScore = curScore + weight})) {
                    result.push_back(nbrNodeID);
                    return;
                }
            }
            // We find an equivalent path.
            while (nbrPathData.pathScore == curScore + weight) {
                if (fwdData.nodePathData[nbrNodeID.offset].compare_exchange_strong(nbrPathData,
                        BCFwdData::PathData{.numPaths = curPaths + nbrPathData.numPaths,
                            .level = std::max(curLevel + 1, nbrPathData.level),
                            .pathScore = curScore + weight})) {
                    return;
                }
            }
        });
        return result;
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<WeightedFwdTraverse>(fwdData);
    }

private:
    BCFwdData& fwdData;
};

class UnweightedFwdTraverse : public EdgeCompute {
public:
    explicit UnweightedFwdTraverse(BCFwdData& fwdData) : fwdData{fwdData} {}

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, graph::NbrScanState::Chunk& chunk,
        bool) override {
        auto curScore = fwdData.nodePathData[boundNodeID.offset].load().pathScore;
        auto curPaths = fwdData.nodePathData[boundNodeID.offset].load().numPaths;
        auto curLevel = fwdData.nodePathData[boundNodeID.offset].load().level;
        std::vector<nodeID_t> result;
        chunk.forEach([&](auto neighbors, auto, auto i) {
            auto nbrNodeID = neighbors[i];
            auto nbrPathData = BCFwdData::PathData{};
            // We see the node for the first time.
            if (fwdData.nodePathData[nbrNodeID.offset].compare_exchange_strong(nbrPathData,
                    BCFwdData::PathData{.numPaths = curPaths,
                        .level = curLevel + 1,
                        .pathScore = curScore + 1})) {
                result.push_back(nbrNodeID);
                return;
            }
            // Reaching here means CAS failed, so nbrPathData is up to date.
            while (nbrPathData.pathScore == curScore + 1) {
                if (fwdData.nodePathData[nbrNodeID.offset].compare_exchange_strong(nbrPathData,
                        BCFwdData::PathData{.numPaths = curPaths + nbrPathData.numPaths,
                            .level = curLevel + 1,
                            .pathScore = curScore + 1})) {
                    return;
                }
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
        auto curDistance = fwdData.nodePathData[boundNodeID.offset].load().pathScore;
        auto curPaths = fwdData.nodePathData[boundNodeID.offset].load().numPaths;
        auto curScore = bwdData.dependencyScores[boundNodeID.offset].load();
        chunk.forEach([&](auto neighbors, auto propertyVectors, auto i) {
            auto nbrNodeID = neighbors[i];
            auto nbrDistance = fwdData.nodePathData[nbrNodeID.offset].load().pathScore;
            const auto weight =
                propertyVectors.empty() ? 1 : propertyVectors[0]->template getValue<double>(i);
            if (nbrDistance + weight == curDistance) {
                auto nbrPaths = fwdData.nodePathData[nbrNodeID.offset].load().numPaths;
                auto scoreToAdd = ((double)nbrPaths / curPaths) * (1 + curScore);
                bwdData.dependencyScores[nbrNodeID.offset].fetch_add(scoreToAdd);
            }
        });
        // We always return empty. We populate the next frontier ourselves.
        // Using a reverse sweep.
        return {};
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<BwdTraverse>(fwdData, bwdData);
    }

private:
    BCFwdData& fwdData;
    BCBwdData& bwdData;
};

class PopulateLevels : public GDSVertexCompute {
public:
    PopulateLevels(NodeOffsetMaskMap* nodeMask, BCFwdData& fwdData)
        : GDSVertexCompute{nodeMask}, fwdData{fwdData} {}

    void beginOnTableInternal(table_id_t) override {}
    void vertexCompute(offset_t startOffset, offset_t endOffset, table_id_t) override {
        for (auto i = startOffset; i < endOffset; ++i) {
            fwdData.levels[i] =
                BCFwdData::LevelData{.node = i, .level = fwdData.nodePathData[i].load().level};
        }
    }
    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<PopulateLevels>(nodeMask, fwdData);
    }

private:
    BCFwdData& fwdData;
};

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
    auto maxIterations = input.bindData->optionalParams->constCast<MaxIterationOptionalParams>()
                             .maxIterations.getParamVal();

    auto currentFrontier = DenseFrontier::getUnvisitedFrontier(input.context, graph);
    auto nextFrontier = DenseFrontier::getUnvisitedFrontier(input.context, graph);
    auto frontierPair =
        std::make_unique<DenseFrontierPair>(std::move(currentFrontier), std::move(nextFrontier));
    auto computeState = GDSComputeState(std::move(frontierPair), nullptr,
        std::make_unique<EmptyGDSAuxiliaryState>());
    for (auto i = 0u; i < numNodes; ++i) {
        // Forward Traverse
        fwdData.init(i /*sourceNode*/);
        computeState.frontierPair->resetCurrentIter();
        computeState.frontierPair->ptrCast<DenseFrontierPair>()->resetValue(input.context, graph,
            FRONTIER_UNVISITED);
        computeState.frontierPair->addNodeToNextFrontier(i);
        if (relProps.empty()) {
            computeState.edgeCompute = std::make_unique<UnweightedFwdTraverse>(fwdData);
        } else {
            computeState.edgeCompute = std::make_unique<WeightedFwdTraverse>(fwdData);
        }
        computeState.frontierPair->setActiveNodesForNextIter();
        GDSUtils::runAlgorithmEdgeCompute(input.context, computeState, graph,
            undirected ? ExtendDirection::BOTH : ExtendDirection::FWD, maxIterations, relProps);
        // Backward Traverse
        bwdData.init();

        const auto populateLevels =
            std::make_unique<PopulateLevels>(sharedState->getGraphNodeMaskMap(), fwdData);
        GDSUtils::runVertexCompute(input.context, GDSDensityState::DENSE, graph, *populateLevels);
        std::sort(fwdData.levels.begin(), fwdData.levels.end(),
            std::greater<BCFwdData::LevelData>{});

        auto maxLevel = fwdData.levels.front().level;
        for (auto j = 0u; j < fwdData.levels.size() && maxLevel > 0;) {
            while (j < fwdData.levels.size() && fwdData.levels[j].level == maxLevel) {
                computeState.frontierPair->addNodeToNextFrontier(fwdData.levels[j].node);
                ++j;
            }
            computeState.edgeCompute = std::make_unique<BwdTraverse>(fwdData, bwdData);
            computeState.frontierPair->setActiveNodesForNextIter();
            GDSUtils::runAlgorithmEdgeCompute(input.context, computeState, graph,
                undirected ? ExtendDirection::BOTH : ExtendDirection::BWD, maxIterations);
            --maxLevel;
        }
        const auto vertexCompute =
            std::make_unique<UpdateBC>(sharedState->getGraphNodeMaskMap(), bwdData, state, i);
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
