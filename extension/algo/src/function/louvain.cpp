#include <cmath>

#include "binder/binder.h"
#include "common/exception/runtime.h"
#include "common/in_mem_gds_utils.h"
#include "common/in_mem_graph.h"
#include "common/string_utils.h"
#include "common/task_system/progress_bar.h"
#include "function/algo_function.h"
#include "function/config/louvain_config.h"
#include "function/config/max_iterations_config.h"
#include "function/gds/gds_utils.h"
#include "function/gds/gds_vertex_compute.h"
#include "processor/execution_context.h"

using namespace std;
using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::storage;
using namespace kuzu::graph;
using namespace kuzu::function;

// Louvain method for community detection: https://en.wikipedia.org/wiki/Louvain_method.
// For nodes in a graph grouped into `C` communities, the modularity of a graph is given as:
//   modularity = sumIntraWeights/2m - (sumWeightedDegrees/2m)^2
// where:
//   sumIntraWeights = sum over all c in C: sum of all edges in c, counted twice in both directions
//                   = sum of the weightedDegree of all nodes considering only edges to neighbors
//                     that are in the same community
//   sumWeightedDegrees = sum over all c in C: (sum of weightedDegree of all nodes in c)^2
//   weightedDegree(n) = sum of the weight of all edges attached to node n

// The parallel Louvain implementation here follows https://hpc.pnl.gov/people/hala/grappolo.html.

namespace kuzu {
namespace algo_extension {

constexpr double THRESHOLD = 1e-6;
constexpr offset_t UNASSIGNED_COMM = numeric_limits<offset_t>::max();

struct LouvainOptionalParams final : public MaxIterationOptionalParams {
    OptionalParam<MaxPhases> maxPhases;

    explicit LouvainOptionalParams(const expression_vector& optionalParams);

    // For copy only
    LouvainOptionalParams(OptionalParam<MaxIterations> maxIterations,
        OptionalParam<MaxPhases> maxPhases)
        : MaxIterationOptionalParams{maxIterations}, maxPhases{std::move(maxPhases)} {}

    void evaluateParams(main::ClientContext* context) override {
        MaxIterationOptionalParams::evaluateParams(context);
        maxPhases.evaluateParam(context);
    }

    std::unique_ptr<function::OptionalParams> copy() override {
        return std::make_unique<LouvainOptionalParams>(maxIterations, maxPhases);
    }
};

LouvainOptionalParams::LouvainOptionalParams(const expression_vector& optionalParams)
    : MaxIterationOptionalParams{constructMaxIterationParam(optionalParams)} {
    for (auto& optionalParam : optionalParams) {
        auto paramName = StringUtils::getLower(optionalParam->getAlias());
        if (paramName == MaxPhases::NAME) {
            maxPhases = function::OptionalParam<MaxPhases>(optionalParam);
        } else if (paramName == MaxIterations::NAME) {
            continue;
        } else {
            throw BinderException{"Unknown optional parameter: " + optionalParam->getAlias()};
        }
    }
}

struct LouvainBindData final : public GDSBindData {
    LouvainBindData(expression_vector columns, graph::NativeGraphEntry graphEntry,
        std::shared_ptr<Expression> nodeOutput,
        std::unique_ptr<LouvainOptionalParams> optionalParams)
        : GDSBindData{std::move(columns), std::move(graphEntry), std::move(nodeOutput)} {
        this->optionalParams = std::move(optionalParams);
    }

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<LouvainBindData>(*this);
    }
};

struct CommInfo {
    std::atomic<offset_t> size;   // The number of nodes in the community.
    std::atomic<weight_t> degree; // The sum of the weighted degree of all nodes in the community.

    CommInfo() : size{0}, degree(0) {}
    CommInfo(const CommInfo& other) {
        size.store(other.size.load());
        degree.store(other.degree.load());
    }
    CommInfo& operator=(const CommInfo& other) {
        if (this != &other) {
            size.store(other.size.load());
            degree.store(other.degree.load());
        }
        return *this;
    }
};

struct PhaseState {
    InMemGraph graph;
    // Latest community assignments that improve the modularity.
    AtomicObjectArray<offset_t> acceptedComm;
    // New community assignments that have not yet been tested for modularity.
    AtomicObjectArray<offset_t> currComm;
    // In-progress changes to community assignments.
    AtomicObjectArray<offset_t> nextComm;
    // Pre-computed size and weighted degree for current community assignments.
    ObjectArray<CommInfo> currCommInfos;
    // Pre-computed size and weighted degree for in-progress changes to community assignments.
    ObjectArray<CommInfo> nextCommInfos;
    // For each node, stores the sum of weights for all its edges.
    AtomicObjectArray<weight_t> nodeWeightedDegrees;
    // For each node, stores the sum of weights of only the edges to nodes in its own community.
    AtomicObjectArray<weight_t> selfCommWeights;
    // Stores 2 * sum of edge weights.
    weight_t totalWeight = 0;
    // 1/2m.
    double modularityConstant = 0.0;

    PhaseState(const offset_t numNodes, MemoryManager* mm, ExecutionContext* context)
        : graph{InMemGraph(numNodes, mm)} {
        reinit(numNodes, mm, context);
    }
    DELETE_BOTH_COPY(PhaseState);

    void reinit(offset_t numNodes, MemoryManager* mm, ExecutionContext* context);

    void startNewIter(MemoryManager* mm, ExecutionContext* context);

    // Initializes the next node in the sequence before inserting edges for the node.
    void initNextNode(const offset_t nodeId) {
        graph.initNextNode();
        // Each community starts with one node.
        currCommInfos.getUnsafe(nodeId).size.store(1, memory_order_relaxed);
        currCommInfos.getUnsafe(nodeId).degree.store(0, memory_order_relaxed);
        // Each node starts in its own community.
        acceptedComm.set(nodeId, nodeId, memory_order_relaxed);
        currComm.set(nodeId, nodeId, memory_order_relaxed);
    }

    // Inserts a neighbor of the last initialized node.
    void insertNbr(const offset_t from, const offset_t to, const weight_t weight = DEFAULT_WEIGHT) {
        graph.insertNbr(to, weight);
        nodeWeightedDegrees.fetchAdd(from, weight, memory_order_relaxed);
        // The weightedDegree of each community is the weightedDegree of its single node.
        currCommInfos.getUnsafe(from).degree.fetch_add(weight, memory_order_relaxed);
        totalWeight += weight;
    }

    void finalize() { graph.initNextNode(); }
};

class ResetPhaseStateVC final : public InMemVertexCompute {
public:
    explicit ResetPhaseStateVC(PhaseState& state) : state{state} {}
    ~ResetPhaseStateVC() override = default;

    void vertexCompute(const offset_t startOffset, const offset_t endOffset) override {
        for (auto nodeId = startOffset; nodeId < endOffset; ++nodeId) {
            state.nodeWeightedDegrees.set(nodeId, 0, memory_order_relaxed);
            state.currCommInfos.set(nodeId, CommInfo());
            state.acceptedComm.set(nodeId, UNASSIGNED_COMM, memory_order_relaxed);
            state.currComm.set(nodeId, UNASSIGNED_COMM, memory_order_relaxed);
            state.nextComm.set(nodeId, UNASSIGNED_COMM, memory_order_relaxed);
        }
    }

    std::unique_ptr<InMemVertexCompute> copy() override {
        return std::make_unique<ResetPhaseStateVC>(state);
    }

private:
    PhaseState& state;
};

class StartNewIterVC final : public InMemVertexCompute {
public:
    explicit StartNewIterVC(PhaseState& state) : state{state} {}
    ~StartNewIterVC() override = default;

    void vertexCompute(const offset_t startOffset, const offset_t endOffset) override {
        for (auto nodeId = startOffset; nodeId < endOffset; ++nodeId) {
            state.selfCommWeights.set(nodeId, 0, memory_order_relaxed);
            state.nextCommInfos.set(nodeId, CommInfo());
        }
    }

    std::unique_ptr<InMemVertexCompute> copy() override {
        return std::make_unique<StartNewIterVC>(state);
    }

private:
    PhaseState& state;
};

void PhaseState::reinit(const offset_t numNodes, MemoryManager* mm, ExecutionContext* context) {
    // All objects reuse allocations because `numNodes` monotonically decreases over phases.
    totalWeight = 0;
    graph.reinit(numNodes);
    nodeWeightedDegrees.reallocate(numNodes, mm);
    currCommInfos.reallocate(numNodes, mm);
    acceptedComm.reallocate(numNodes, mm);
    currComm.reallocate(numNodes, mm);
    nextComm.reallocate(numNodes, mm);

    ResetPhaseStateVC resetPhaseStateVC(*this);
    InMemGDSUtils::runVertexCompute(resetPhaseStateVC, numNodes, context);
}

void PhaseState::startNewIter(MemoryManager* mm, ExecutionContext* context) {
    selfCommWeights.reallocate(graph.numNodes, mm);
    nextCommInfos.reallocate(graph.numNodes, mm);

    StartNewIterVC startNewIterVC(*this);
    InMemGDSUtils::runVertexCompute(startNewIterVC, graph.numNodes, context);

    modularityConstant = 1.0 / totalWeight;
}

struct FinalResults {
    vector<offset_t> communities;

    explicit FinalResults(const offset_t numNodes) { communities.resize(numNodes); }
};

class SaveCommAssignmentsVC final : public InMemVertexCompute {
public:
    explicit SaveCommAssignmentsVC(const offset_t phaseId, FinalResults& finalResults,
        PhaseState& state)
        : phaseId{phaseId}, finalResults{finalResults}, state{state} {}
    ~SaveCommAssignmentsVC() override = default;

    void vertexCompute(const offset_t startOffset, const offset_t endOffset) override {
        if (phaseId == 0) {
            for (auto nodeId = startOffset; nodeId < endOffset; ++nodeId) {
                finalResults.communities[nodeId] =
                    state.acceptedComm.get(nodeId, memory_order_relaxed);
            }
        } else {
            for (auto nodeId = startOffset; nodeId < endOffset; ++nodeId) {
                const auto prevCommunity = finalResults.communities[nodeId];
                if (prevCommunity == UNASSIGNED_COMM) {
                    continue;
                }
                // Every previous community becomes a node in the current phase.
                const auto newCommunity =
                    state.acceptedComm.get(prevCommunity, memory_order_relaxed);
                finalResults.communities[nodeId] = newCommunity;
            }
        }
    }

    std::unique_ptr<InMemVertexCompute> copy() override {
        return std::make_unique<SaveCommAssignmentsVC>(phaseId, finalResults, state);
    }

private:
    offset_t phaseId;
    FinalResults& finalResults;
    PhaseState& state;
};

class RunIterationVC final : public InMemVertexCompute {
public:
    explicit RunIterationVC(PhaseState& state) : state{state} {}
    ~RunIterationVC() override = default;

    void vertexCompute(const offset_t startOffset, const offset_t endOffset) override {
        // For every `nodeId`, separately stores the edge weights to its own community (at index 0)
        // and each of its neighboring communities.
        vector<weight_t> intraCommWeights;
        // Stores the mapping from communities to an index in `intraCommWeights`.
        unordered_map<offset_t, offset_t> commToWeightsIndex;
        for (auto nodeId = startOffset; nodeId < endOffset; ++nodeId) {
            const auto startCSROffset = state.graph.csrOffsets[nodeId];
            const auto endCSROffset = state.graph.csrOffsets[nodeId + 1];
            offset_t targetCommId = UNASSIGNED_COMM;
            if (startCSROffset != endCSROffset) {
                commToWeightsIndex.clear();
                intraCommWeights.clear();
                const weight_t selfLoopWeight = computeIntraCommWeights(nodeId, startCSROffset,
                    endCSROffset, intraCommWeights, commToWeightsIndex);
                targetCommId = findPotentialNewComm(nodeId, selfLoopWeight, intraCommWeights,
                    commToWeightsIndex);
                // Store the current community weight for use in modularity calculation.
                state.selfCommWeights.set(nodeId, intraCommWeights[0], memory_order_relaxed);
            }
            state.nextComm.set(nodeId, targetCommId, memory_order_relaxed);

            const auto currCommId = state.currComm.get(nodeId, memory_order_relaxed);
            if (targetCommId != currCommId && targetCommId != UNASSIGNED_COMM) {
                const auto nodeDegree = state.nodeWeightedDegrees.get(nodeId, memory_order_relaxed);
                // Add node contribution to the new community.
                state.nextCommInfos.getUnsafe(targetCommId).degree.fetch_add(nodeDegree);
                state.nextCommInfos.getUnsafe(targetCommId).size.fetch_add(1);
                // Remove node contribution from the current community.
                state.nextCommInfos.getUnsafe(currCommId).degree.fetch_sub(nodeDegree);
                state.nextCommInfos.getUnsafe(currCommId).size.fetch_sub(1);
            }
        }
    }

    // Compute the intra community weights for each community `nodeId` can be moved to.
    weight_t computeIntraCommWeights(const offset_t nodeId, const offset_t startCSROffset,
        const offset_t endCSROffset, vector<weight_t>& intraCommWeights,
        unordered_map<offset_t, offset_t>& commToWeightsIndex) const {
        // Self-loop weight remains the same when moving between communities. Store this
        // separately for use in the modularity "diff" calculation.
        weight_t selfLoopWeight = 0;
        const auto currComm = state.currComm.get(nodeId, memory_order_relaxed);
        commToWeightsIndex[currComm] = 0;
        intraCommWeights.push_back(0);
        offset_t nextIndex = 1;
        for (auto offset = startCSROffset; offset < endCSROffset; offset++) {
            auto nbrEntry = state.graph.csrEdges[offset];
            if (nbrEntry.neighbor == nodeId) {
                selfLoopWeight += nbrEntry.weight;
            }
            auto nbrCommId = state.currComm.get(nbrEntry.neighbor, memory_order_relaxed);
            if (!commToWeightsIndex.contains(nbrCommId)) {
                // Found a new neighbor community.
                commToWeightsIndex[nbrCommId] = nextIndex;
                nextIndex++;
                intraCommWeights.push_back(nbrEntry.weight);
            } else {
                intraCommWeights[commToWeightsIndex[nbrCommId]] += nbrEntry.weight;
            }
        }
        return selfLoopWeight;
    }

    // Find `nodeId`'s community that maximizes the graph modularity gain.
    offset_t findPotentialNewComm(const offset_t nodeId, const weight_t selfLoopWeight,
        const vector<weight_t>& intraCommWeights,
        unordered_map<offset_t, offset_t> commToWeightsIndex) const {
        const auto currComm = state.currComm.get(nodeId, memory_order_relaxed);
        const auto degree =
            static_cast<double>(state.nodeWeightedDegrees.get(nodeId, memory_order_relaxed));
        auto newComm = currComm;
        double newCommModGain = 0.0;
        const auto prevIntraCommWeights = static_cast<double>(intraCommWeights[0] - selfLoopWeight);
        const auto prevWeightedDegrees =
            static_cast<double>(
                state.currCommInfos.getUnsafe(currComm).degree.load(memory_order_relaxed)) -
            degree;
        for (auto [nbrCommId, weightIndex] : commToWeightsIndex) {
            if (currComm != nbrCommId) {
                const auto newIntraCommWeights = static_cast<double>(intraCommWeights[weightIndex]);
                const auto newWeightedDegrees = static_cast<double>(
                    state.currCommInfos.getUnsafe(nbrCommId).degree.load(memory_order_relaxed));
                // The change in gain is computed as the change in the two parts:
                //   sumIntraWeights/2m - sumWeightedDegrees^2/(2m)^2
                // If moving node n from c to a new community d:
                // sumIntraWeights before move =
                //   (selfLoop_n+2*(edges_n in c)+(other nodes in c)+(other nodes in d) + ...)/2m
                // sumIntraWeights after move =
                //   ((other nodes in c)+selfLoop_n+2*(edges_n in d)+(other nodes in d) + ...)/2m
                // sumIntraWeights diff =
                //   (2*(edges_n in d) - 2*(edges_n in c))/2m
                const auto changeIntraWeights = 2 * (newIntraCommWeights - prevIntraCommWeights);
                // sumWeightedDegrees before move =
                //   ((degree_n+degree_{other nodes in c})^2+(degree_{other nodes in d})^2)/(2m)^2
                // sumWeightedDegrees after move =
                //   ((degree_{other nodes in c})^2+(degree_n+degree_{other nodes in d})^2)/(2m)^2
                // sumWeightedDegrees diff =
                //   2*degree_n*(degree_{other nodes in d}-degree_{other nodes in c})/(2m)^2
                const auto changeSumWeightedDegrees = 2 * degree * state.modularityConstant *
                                                      (newWeightedDegrees - prevWeightedDegrees);
                // Both sides multiplied by 2*m to reduce constants.
                const auto modGain = changeIntraWeights - changeSumWeightedDegrees;
                if (modGain > newCommModGain || ((newCommModGain - modGain) < THRESHOLD &&
                                                    modGain != 0 && (nbrCommId < newComm))) {
                    // Move if gain is higher, or gain is the same, but nbrComm has a lower ID.
                    newCommModGain = modGain;
                    newComm = nbrCommId;
                }
            }
        }
        if (state.currCommInfos.getUnsafe(newComm).size.load(memory_order_relaxed) == 1 &&
            state.currCommInfos.getUnsafe(currComm).size.load(memory_order_relaxed) == 1 &&
            newComm > currComm) { // Swap protection
            newComm = currComm;
        }
        return newComm;
    }

    std::unique_ptr<InMemVertexCompute> copy() override {
        return std::make_unique<RunIterationVC>(state);
    }

private:
    PhaseState& state;
};

class ComputeModularityVC final : public InMemVertexCompute {
public:
    ComputeModularityVC(PhaseState& state, std::atomic<weight_t>& sumIntraWeights,
        std::atomic<weight_t>& sumWeightedDegrees)
        : state{state}, sumIntraWeights{sumIntraWeights}, sumWeightedDegrees{sumWeightedDegrees} {}
    ~ComputeModularityVC() override = default;

    void vertexCompute(const offset_t startOffset, const offset_t endOffset) override {
        weight_t sumIntraLocal = 0;
        weight_t sumTotalLocal = 0;
        for (auto nodeId = startOffset; nodeId < endOffset; ++nodeId) {
            sumIntraLocal += state.selfCommWeights.get(nodeId, memory_order_relaxed);
            const auto degree =
                state.currCommInfos.getUnsafe(nodeId).degree.load(memory_order_relaxed);
            sumTotalLocal += degree * degree;
        }
        sumIntraWeights.fetch_add(sumIntraLocal);
        sumWeightedDegrees.fetch_add(sumTotalLocal);
    }

    std::unique_ptr<InMemVertexCompute> copy() override {
        return std::make_unique<ComputeModularityVC>(state, sumIntraWeights, sumWeightedDegrees);
    }

private:
    PhaseState& state;
    std::atomic<weight_t>& sumIntraWeights;
    std::atomic<weight_t>& sumWeightedDegrees;
};

class UpdateCommInfosVC final : public InMemVertexCompute {
public:
    explicit UpdateCommInfosVC(PhaseState& state) : state{state} {}
    ~UpdateCommInfosVC() override = default;

    void vertexCompute(const offset_t startOffset, const offset_t endOffset) override {
        for (auto nodeId = startOffset; nodeId < endOffset; ++nodeId) {
            const offset_t size =
                state.nextCommInfos.getUnsafe(nodeId).size.load(memory_order_relaxed);
            const weight_t degree =
                state.nextCommInfos.getUnsafe(nodeId).degree.load(memory_order_relaxed);
            state.currCommInfos.getUnsafe(nodeId).size.fetch_add(size, memory_order_relaxed);
            state.currCommInfos.getUnsafe(nodeId).degree.fetch_add(degree, memory_order_relaxed);
        }
    }

    std::unique_ptr<InMemVertexCompute> copy() override {
        return std::make_unique<UpdateCommInfosVC>(state);
    }

private:
    PhaseState& state;
};

class WriteResultsVC final : public GDSResultVertexCompute {
public:
    WriteResultsVC(MemoryManager* mm, GDSFuncSharedState* sharedState, FinalResults& louvainState)
        : GDSResultVertexCompute{mm, sharedState}, finalResults{louvainState} {
        nodeIDVector = createVector(LogicalType::INTERNAL_ID());
        componentIDVector = createVector(LogicalType::UINT64());
    }

    void beginOnTableInternal(table_id_t /*tableID*/) override {}

    void vertexCompute(const offset_t startOffset, const offset_t endOffset,
        const table_id_t tableID) override {
        for (auto i = startOffset; i < endOffset; ++i) {
            const auto nodeID = nodeID_t{i, tableID};
            nodeIDVector->setValue<nodeID_t>(0, nodeID);
            componentIDVector->setValue<uint64_t>(0, finalResults.communities[i]);
            localFT->append(vectors);
        }
    }

    unique_ptr<VertexCompute> copy() override {
        return std::make_unique<WriteResultsVC>(mm, sharedState, finalResults);
    }

private:
    FinalResults& finalResults;
    unique_ptr<ValueVector> nodeIDVector;
    unique_ptr<ValueVector> componentIDVector;
};

void initInMemoryGraph(const table_id_t tableId, const offset_t numNodes, Graph* graph,
    PhaseState& state) {
    const auto nbrTables = graph->getRelInfos(tableId);
    const auto nbrInfo = nbrTables[0];
    KU_ASSERT(nbrInfo.srcTableID == nbrInfo.dstTableID);
    const auto scanState =
        graph->prepareRelScan(*nbrInfo.relGroupEntry, nbrInfo.relTableID, nbrInfo.dstTableID, {});

    for (auto nodeId = 0u; nodeId < numNodes; ++nodeId) {
        state.initNextNode(nodeId);
        const nodeID_t nextNodeId = {nodeId, tableId};
        for (auto chunk : graph->scanFwd(nextNodeId, *scanState)) {
            chunk.forEach([&](auto neighbors, auto, auto i) {
                auto nbrId = neighbors[i].offset;
                state.insertNbr(nodeId, nbrId);
            });
        }
        for (auto chunk : graph->scanBwd(nextNodeId, *scanState)) {
            chunk.forEach([&](auto neighbors, auto, auto i) {
                auto nbrId = neighbors[i].offset;
                if (nbrId != nodeId) {
                    state.insertNbr(nodeId, nbrId);
                }
            });
        }
    }
    state.finalize();
}

// Sequentially renumber the communities, each of which becomes a new node in the next phase.
offset_t renumberCommunities(PhaseState& state) {
    unordered_map<offset_t, offset_t> map;
    offset_t nextCommId = 0;
    for (auto nodeId = 0LU; nodeId < state.graph.numNodes; ++nodeId) {
        auto commId = state.acceptedComm.get(nodeId, memory_order_relaxed);
        if (commId == UNASSIGNED_COMM) {
            // Skip creating communities for isolated nodes.
            continue;
        }
        if (!map.contains(commId)) {
            map.insert(make_pair(commId, nextCommId));
            nextCommId++;
        }
        state.acceptedComm.set(nodeId, map.at(commId), memory_order_relaxed);
    }
    return nextCommId;
}

void aggregateCommunities(const offset_t newCommCount, PhaseState& state, MemoryManager* mm,
    ExecutionContext* context) {
    ku_vector_t<unordered_map<offset_t, weight_t>> commWeights(mm);
    commWeights.resize(newCommCount);
    for (auto nodeId = 0u; nodeId < state.graph.numNodes; nodeId++) {
        const auto beginCSROffset = state.graph.csrOffsets[nodeId];
        const auto endCSROffset = state.graph.csrOffsets[nodeId + 1];
        auto commId = state.acceptedComm.get(nodeId, memory_order_relaxed);
        for (auto offset = beginCSROffset; offset < endCSROffset; ++offset) {
            const auto nbr = state.graph.csrEdges[offset];
            auto nbrCommId = state.acceptedComm.get(nbr.neighbor, memory_order_relaxed);
            if (commId >= nbrCommId) {
                // New forward edge.
                commWeights[commId][nbrCommId] += nbr.weight;
                if (commId != nbrCommId) {
                    // New backward edge, skipping self-loops.
                    commWeights[nbrCommId][commId] += nbr.weight;
                }
            }
        }
    }
    state.reinit(newCommCount, mm, context);
    for (auto nodeId = 0u; nodeId < newCommCount; nodeId++) {
        state.initNextNode(nodeId);
        for (auto [nbrId, weight] : commWeights[nodeId]) {
            state.insertNbr(nodeId, nbrId, weight);
        }
    }
    state.finalize();
}

static common::offset_t tableFunc(const TableFuncInput& input, TableFuncOutput&) {
    const auto clientContext = input.context->clientContext;
    const auto transaction = clientContext->getTransaction();
    auto sharedState = input.sharedState->ptrCast<GDSFuncSharedState>();
    auto mm = clientContext->getMemoryManager();
    const auto graph = sharedState->graph.get();
    auto maxOffsetMap = graph->getMaxOffsetMap(transaction);
    KU_ASSERT(graph->getNodeTableIDs().size() == 1);
    const auto tableID = graph->getNodeTableIDs()[0];
    const auto origNumNodes = graph->getMaxOffset(clientContext->getTransaction(), tableID);

    auto louvainBindData = input.bindData->constPtrCast<LouvainBindData>();
    auto& config = louvainBindData->optionalParams->constCast<LouvainOptionalParams>();

    auto progressBar = clientContext->getProgressBar();
    const auto steps = config.maxPhases.getParamVal() * config.maxIterations.getParamVal();

    FinalResults finalResults(origNumNodes);
    PhaseState state(origNumNodes, mm, input.context);

    // Create the initial in-memory graph.
    initInMemoryGraph(tableID, origNumNodes, graph, state);

    // Each phases attempts to decrease the number of communities by merging nodes into supernodes.
    for (auto phase = 0u; phase < config.maxPhases.getParamVal(); ++phase) {
        double oldMod = -1;

        // Each iteration attempts to increase the modularity by moving nodes to new communities.
        for (auto iter = 0u; iter < config.maxIterations.getParamVal(); ++iter) {
            double progress = static_cast<double>((phase + 1) * (iter + 1)) / steps;

            // Reset state.
            state.startNewIter(mm, input.context);

            // For each node, try to find a neighbor community such that moving the node to that
            // community increases the graph modularity. Note that the new community assignments are
            // sensitive to the order in which the nodes are processed.
            RunIterationVC runIteration(state);
            InMemGDSUtils::runVertexCompute(runIteration, state.graph.numNodes, input.context);

            progressBar->updateProgress(input.context->queryID, progress * 0.5);

            // Compute the modularity for the _current_ node communities, i.e., *before* the
            // node movements above. This keeps the logic for computing `sumIntraWeights` simpler.
            std::atomic<weight_t> sumIntraWeights{0};
            std::atomic<weight_t> sumWeightedDegrees{0};
            ComputeModularityVC newModularityVC(state, sumIntraWeights, sumWeightedDegrees);
            InMemGDSUtils::runVertexCompute(newModularityVC, state.graph.numNodes, input.context);
            const double currMod =
                sumIntraWeights.load() * state.modularityConstant -
                (sumWeightedDegrees.load() * state.modularityConstant * state.modularityConstant);

            if (currMod - oldMod < THRESHOLD) {
                // The community assignments in `currComm` don't increase the modularity. The
                // assignments in `acceptedComm` are the final assignments for this phase.
                break;
            }

            oldMod = currMod;
            // nextCommInfo -> currCommInfo.
            UpdateCommInfosVC updateCommInfosVC(state);
            InMemGDSUtils::runVertexCompute(updateCommInfosVC, state.graph.numNodes, input.context);

            // Save `currComm` to `acceptedComm`.
            std::swap(state.acceptedComm, state.currComm);
            // `nextComm` will be tested for modularity change in the next iteration.
            std::swap(state.currComm, state.nextComm);

            progressBar->updateProgress(input.context->queryID, progress);
        }
        const auto oldCommCount = state.graph.numNodes;
        const auto newCommCount = renumberCommunities(state);

        // Save the renumbered communities as output.
        SaveCommAssignmentsVC setFinalComms(phase, finalResults, state);
        InMemGDSUtils::runVertexCompute(setFinalComms, origNumNodes, input.context);

        if (oldCommCount == newCommCount) {
            // No node merged into a neighbor community. The assignments saved above are final.
            break;
        }

        // Construct a new in-memory graph such that all nodes assigned to the same community are
        // merged into a supernode, and the weights of edges within and across communities are
        // summed up appropriately.
        aggregateCommunities(newCommCount, state, mm, input.context);
    }

    const auto vertexCompute = make_unique<WriteResultsVC>(mm, sharedState, finalResults);
    GDSUtils::runVertexCompute(input.context, GDSDensityState::DENSE, graph, *vertexCompute);

    sharedState->factorizedTablePool.mergeLocalTables();
    return 0;
}

static constexpr char LOUVAIN_ID_COLUMN_NAME[] = "louvain_id";

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    const auto graphName = input->getLiteralVal<std::string>(0);
    auto graphEntry = GDSFunction::bindGraphEntry(*context, graphName);
    if (graphEntry.nodeInfos.size() != 1) {
        throw RuntimeException("Louvain only supports operations on one node table.");
    }
    if (graphEntry.relInfos.size() != 1) {
        throw RuntimeException("Louvain only supports operations on one edge table.");
    }
    expression_vector columns;
    auto nodeOutput = GDSFunction::bindNodeOutput(*input, graphEntry.getNodeEntries());
    columns.push_back(nodeOutput->constPtrCast<NodeExpression>()->getInternalID());
    columns.push_back(input->binder->createVariable(LOUVAIN_ID_COLUMN_NAME, LogicalType::INT64()));
    return std::make_unique<LouvainBindData>(std::move(columns), std::move(graphEntry), nodeOutput,
        std::make_unique<LouvainOptionalParams>(input->optionalParamsLegacy));
}

function_set LouvainFunction::getFunctionSet() {
    function_set result;
    auto func = std::make_unique<TableFunction>(name, std::vector{LogicalTypeID::ANY});
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
