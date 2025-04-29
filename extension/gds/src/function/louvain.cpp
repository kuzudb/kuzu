#include <cmath>

#include "binder/binder.h"
#include "common/exception/runtime.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/gds_utils.h"
#include "function/gds/gds_utils_mem.h"
#include "function/gds/gds_vertex_compute.h"
#include "function/gds_function.h"
#include "graph/graph_mem.h"
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
namespace gds_extension {

constexpr uint32_t MAX_PHASES = 100;
constexpr uint32_t MAX_ITERATIONS = 100;
constexpr double THRESHOLD = 1e-6;
static constexpr offset_t UNASSIGNED_COMM = numeric_limits<offset_t>::max();

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
    AtomicObjectArray<weight_t> nodeWeightedDegrees;
    ObjectArray<CommInfo> currCommInfos;
    ObjectArray<CommInfo> nextCommInfos;
    AtomicObjectArray<offset_t> previousComm;
    AtomicObjectArray<offset_t> currComm;
    AtomicObjectArray<offset_t> nextComm;
    AtomicObjectArray<weight_t> intraCommWeights;
    weight_t totalWeight = 0;        // Stores 2 * sum of edge weights.
    double modularityConstant = 0.0; // 1/2m.

    PhaseState(const offset_t numNodes, MemoryManager* mm, ExecutionContext* context)
        : graph{InMemGraph(numNodes)} {
        reset(numNodes, mm, context);
    }
    DELETE_BOTH_COPY(PhaseState);

    void reset(offset_t numNodes, MemoryManager* mm, ExecutionContext* context);

    void startNewIter(MemoryManager* mm, ExecutionContext* context);

    // Initialize the next node in sequence. Should be called before inserting edges for the node.
    void initNextNode(const offset_t nodeId) {
        graph.initNextNode();
        // Each community starts with one node.
        currCommInfos.getRef(nodeId).size.store(1, memory_order_relaxed);
        currCommInfos.getRef(nodeId).degree.store(0, memory_order_relaxed);
        // Each node starts in its own community.
        previousComm.set(nodeId, nodeId, memory_order_relaxed);
        currComm.set(nodeId, nodeId, memory_order_relaxed);
    }

    // Insert a neighbor of the last initialized node. Edges should be inserted in both directions.
    void insertNbr(const offset_t from, const offset_t to, const weight_t weight = DEFAULT_WEIGHT) {
        graph.insertNbr(to, weight);
        nodeWeightedDegrees.fetchAdd(from, weight, memory_order_relaxed);
        // The weightedDegree of each community is the weightedDegree of its single node.
        currCommInfos.getRef(from).degree.fetch_add(weight, memory_order_relaxed);
        totalWeight += weight;
    }

    void finalize() { graph.initNextNode(); }
};

class ResetPhaseStateVC : public InMemVertexCompute {
public:
    explicit ResetPhaseStateVC(PhaseState& state) : state{state} {}

    void vertexCompute(offset_t startOffset, offset_t endOffset) override {
        for (auto nodeId = startOffset; nodeId < endOffset; ++nodeId) {
            state.nodeWeightedDegrees.set(nodeId, 0, memory_order_relaxed);
            state.currCommInfos.set(nodeId, CommInfo());
            state.previousComm.set(nodeId, UNASSIGNED_COMM, memory_order_relaxed);
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

class StartNewIterVC : public InMemVertexCompute {
public:
    explicit StartNewIterVC(PhaseState& state) : state{state} {}

    void vertexCompute(offset_t startOffset, offset_t endOffset) override {
        for (auto nodeId = startOffset; nodeId < endOffset; ++nodeId) {
            state.intraCommWeights.set(nodeId, 0, memory_order_relaxed);
            state.nextCommInfos.set(nodeId, CommInfo());
        }
    }

    std::unique_ptr<InMemVertexCompute> copy() override {
        return std::make_unique<StartNewIterVC>(state);
    }

private:
    PhaseState& state;
};

void PhaseState::reset(const offset_t numNodes, MemoryManager* mm, ExecutionContext* context) {
    // Reuses allocations because `numNodes` monotonically decreases over phases.
    totalWeight = 0;
    graph.reset(numNodes);
    nodeWeightedDegrees.resizeAsNew(numNodes, mm);
    currCommInfos.resizeAsNew(numNodes, mm);
    previousComm.resizeAsNew(numNodes, mm);
    currComm.resizeAsNew(numNodes, mm);
    nextComm.resizeAsNew(numNodes, mm);

    ResetPhaseStateVC resetPhaseStateVC(*this);
    InMemGDSUtils::runVertexCompute(resetPhaseStateVC, numNodes, context);
}

void PhaseState::startNewIter(MemoryManager* mm, ExecutionContext* context) {
    intraCommWeights.resizeAsNew(graph.numNodes, mm);
    nextCommInfos.resizeAsNew(graph.numNodes, mm);

    StartNewIterVC startNewIterVC(*this);
    InMemGDSUtils::runVertexCompute(startNewIterVC, graph.numNodes, context);

    modularityConstant = 1.0 / totalWeight;
}

struct FinalResults {
    vector<offset_t> communities;

    explicit FinalResults(const offset_t numNodes) { communities.resize(numNodes); }
};

class SaveCommAssignmentsVC : public InMemVertexCompute {
public:
    explicit SaveCommAssignmentsVC(offset_t phaseId, FinalResults& finalResults, PhaseState& state)
        : phaseId{phaseId}, finalResults{finalResults}, state{state} {}

    void vertexCompute(offset_t startOffset, offset_t endOffset) override {
        if (phaseId == 0) {
            for (auto nodeId = startOffset; nodeId < endOffset; ++nodeId) {
                finalResults.communities[nodeId] =
                    state.previousComm.get(nodeId, memory_order_relaxed);
            }
        } else {
            for (auto nodeId = startOffset; nodeId < endOffset; ++nodeId) {
                auto prevCommunity = finalResults.communities[nodeId];
                if (prevCommunity == UNASSIGNED_COMM) {
                    continue;
                }
                // Every previous community becomes a node in the current phase.
                auto newCommunity = state.previousComm.get(prevCommunity, memory_order_relaxed);
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

class RunIterationVC : public InMemVertexCompute {
public:
    explicit RunIterationVC(PhaseState& state) : state{state} {}

    void vertexCompute(offset_t startOffset, offset_t endOffset) override {
        for (auto nodeId = startOffset; nodeId < endOffset; ++nodeId) {
            auto startCSROffset = state.graph.csrOffsets[nodeId];
            auto endCSROffset = state.graph.csrOffsets[nodeId + 1];
            // Self loop weight remains the same when moving between communities. Store this
            // separately for use in the modularity "diff" calculation.
            weight_t selfLoopWeight = 0;
            vector<weight_t> intraCommWeights;
            unordered_map<offset_t, offset_t> commToWeightsIndex;
            offset_t targetCommId = UNASSIGNED_COMM;
            if (startCSROffset != endCSROffset) {
                selfLoopWeight = computeIntraCommWeights(nodeId, startCSROffset, endCSROffset,
                    intraCommWeights, commToWeightsIndex);
                targetCommId = findPotentialNewComm(nodeId, selfLoopWeight, intraCommWeights,
                    commToWeightsIndex);
                // Store the current intra-community weight for use in modularity calculation.
                state.intraCommWeights.set(nodeId, intraCommWeights[0], memory_order_relaxed);
            }
            state.nextComm.set(nodeId, targetCommId, memory_order_relaxed);

            auto currCommId = state.currComm.get(nodeId, memory_order_relaxed);
            if (targetCommId != currCommId && targetCommId != UNASSIGNED_COMM) {
                auto nodeDegree = state.nodeWeightedDegrees.get(nodeId, memory_order_relaxed);
                // Add node contribution to the new community.
                state.nextCommInfos.getRef(targetCommId).degree.fetch_add(nodeDegree);
                state.nextCommInfos.getRef(targetCommId).size.fetch_add(1);
                // Remove node contribution from the old community.
                state.nextCommInfos.getRef(currCommId).degree.fetch_sub(nodeDegree);
                state.nextCommInfos.getRef(currCommId).size.fetch_sub(1);
            }
            commToWeightsIndex.clear();
            intraCommWeights.clear();
        }
    }

    // Compute the intra community weights for each community `nodeId` can be moved to.
    weight_t computeIntraCommWeights(offset_t nodeId, offset_t startCSROffset,
        offset_t endCSROffset, vector<weight_t>& intraCommWeights,
        unordered_map<offset_t, offset_t>& commToWeightsIndex) const {
        weight_t selfLoopWeight = 0;
        auto currComm = state.currComm.get(nodeId, memory_order_relaxed);
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

    // Find `nodeId`'s community that maximises the graph modularity gain.
    offset_t findPotentialNewComm(offset_t nodeId, weight_t selfLoopWeight,
        vector<weight_t>& intraCommWeights,
        unordered_map<offset_t, offset_t> commToWeightsIndex) const {
        auto currComm = state.currComm.get(nodeId, memory_order_relaxed);
        auto degree =
            static_cast<double>(state.nodeWeightedDegrees.get(nodeId, memory_order_relaxed));
        auto newComm = currComm;
        double newCommModGain = 0.0;
        auto prevIntraCommWeights = static_cast<double>(intraCommWeights[0] - selfLoopWeight);
        auto prevWeightedDegrees =
            static_cast<double>(
                state.currCommInfos.getRef(currComm).degree.load(memory_order_relaxed)) -
            degree;
        for (auto [nbrCommId, weightIndex] : commToWeightsIndex) {
            if (currComm != nbrCommId) {
                auto newIntraCommWeights = static_cast<double>(intraCommWeights[weightIndex]);
                auto newWeightedDegrees = static_cast<double>(
                    state.currCommInfos.getRef(nbrCommId).degree.load(memory_order_relaxed));
                // The change in gain is computed as the change in the two components:
                //   sumIntraWeights/2m - sumWeightedDegrees^2/(2m)^2
                // If moving node n from c to a new community d:
                // sumIntraWeights before move =
                //   (selfLoop_n+2*(edges_n in c)+(other nodes in c)+(other nodes in d) + ...)/2m
                // sumIntraWeights after move =
                //   ((other nodes in c)+selfLoop_n+2*(edges_n in d)+(other nodes in d) + ...)/2m
                // sumIntraWeights diff =
                //   (2*(edges_n in d) - 2*(edges_n in c))/2m
                auto changeIntraWeights = 2 * (newIntraCommWeights - prevIntraCommWeights);
                // sumWeightedDegrees before move =
                //   ((degree_n+degree_{other nodes in c})^2+(degree_{other nodes in d})^2)/(2m)^2
                // sumWeightedDegrees after move =
                //   ((degree_{other nodes in c})^2+(degree_n+degree_{other nodes in d})^2)/(2m)^2
                // sumWeightedDegrees diff =
                //   2*degree_n*(degree_{other nodes in d}-degree_{other nodes in c})/(2m)^2
                auto changeSumWeightedDegrees = 2 * degree * state.modularityConstant *
                                                (newWeightedDegrees - prevWeightedDegrees);
                // Both sides multiplied by 2m to reduce constants.
                auto modGain = changeIntraWeights - changeSumWeightedDegrees;
                if (modGain > newCommModGain || ((newCommModGain - modGain) < THRESHOLD &&
                                                    modGain != 0 && (nbrCommId < newComm))) {
                    // Move if gain is higher, or gain is the same but nbrComm has a lower ID.
                    newCommModGain = modGain;
                    newComm = nbrCommId;
                }
            }
        }
        if (state.currCommInfos.getRef(newComm).size.load(memory_order_relaxed) == 1 &&
            state.currCommInfos.getRef(currComm).size.load(memory_order_relaxed) == 1 &&
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

class ComputeModularityVC : public InMemVertexCompute {
public:
    static std::atomic<weight_t> sumIntraWeights;
    static std::atomic<weight_t> sumWeightedDegrees;

    explicit ComputeModularityVC(PhaseState& state) : state{state} {}

    static void reset() {
        sumIntraWeights.store(0);
        sumWeightedDegrees.store(0);
    }

    void vertexCompute(offset_t startOffset, offset_t endOffset) override {
        weight_t sumIntraLocal = 0;
        weight_t sumTotalLocal = 0;
        for (auto nodeId = startOffset; nodeId < endOffset; ++nodeId) {
            sumIntraLocal += state.intraCommWeights.get(nodeId, memory_order_relaxed);
            auto degree = state.currCommInfos.getRef(nodeId).degree.load(memory_order_relaxed);
            sumTotalLocal += degree * degree;
        }
        sumIntraWeights.fetch_add(sumIntraLocal);
        sumWeightedDegrees.fetch_add(sumTotalLocal);
    }

    std::unique_ptr<InMemVertexCompute> copy() override {
        return std::make_unique<ComputeModularityVC>(state);
    }

private:
    PhaseState& state;
};
std::atomic<weight_t> ComputeModularityVC::sumIntraWeights{0};
std::atomic<weight_t> ComputeModularityVC::sumWeightedDegrees{0};

class UpdateCommInfosVC : public InMemVertexCompute {
public:
    explicit UpdateCommInfosVC(PhaseState& state) : state{state} {}

    void vertexCompute(offset_t startOffset, offset_t endOffset) override {
        for (auto nodeId = startOffset; nodeId < endOffset; ++nodeId) {
            offset_t size = state.nextCommInfos.getRef(nodeId).size.load(memory_order_relaxed);
            weight_t degree = state.nextCommInfos.getRef(nodeId).degree.load(memory_order_relaxed);
            state.currCommInfos.getRef(nodeId).size.fetch_add(size, memory_order_relaxed);
            state.currCommInfos.getRef(nodeId).degree.fetch_add(degree, memory_order_relaxed);
        }
    }

    std::unique_ptr<InMemVertexCompute> copy() override {
        return std::make_unique<UpdateCommInfosVC>(state);
    }

private:
    PhaseState& state;
};

class WriteResultsVC : public GDSResultVertexCompute {
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
            auto nodeID = nodeID_t{i, tableID};
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
    auto nbrTables = graph->getForwardNbrTableInfos(tableId);
    auto nbrInfo = nbrTables[0];
    auto scanState = graph->prepareRelScan(nbrInfo.relEntry, nbrInfo.nodeEntry, {});

    for (auto nodeId = 0u; nodeId < numNodes; ++nodeId) {
        state.initNextNode(nodeId);
        nodeID_t nextNodeId = {nodeId, tableId};
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
        auto commId = state.previousComm.get(nodeId, memory_order_relaxed);
        if (commId == UNASSIGNED_COMM) {
            // Skip creating communities for isolated nodes.
            continue;
        }
        if (!map.contains(commId)) {
            map.insert(make_pair(commId, nextCommId));
            nextCommId++;
        }
        state.previousComm.set(nodeId, map.at(commId), memory_order_relaxed);
    }
    return nextCommId;
}

void aggregateCommunities(offset_t newCommCount, PhaseState& state, MemoryManager* mm,
    ExecutionContext* context) {
    vector<unordered_map<offset_t, weight_t>> commWeights;
    commWeights.resize(newCommCount);
    for (auto nodeId = 0u; nodeId < state.graph.numNodes; nodeId++) {
        auto beginCSROffset = state.graph.csrOffsets[nodeId];
        auto endCSROffset = state.graph.csrOffsets[nodeId + 1];
        auto commId = state.previousComm.get(nodeId, memory_order_relaxed);
        for (auto offset = beginCSROffset; offset < endCSROffset; ++offset) {
            auto nbr = state.graph.csrEdges[offset];
            auto nbrCommId = state.previousComm.get(nbr.neighbor, memory_order_relaxed);
            if (commId >= nbrCommId) {
                // New forward edge.
                commWeights[commId][nbrCommId] += nbr.weight;
                if (commId != nbrCommId) {
                    // New backward edge, skipping self loops.
                    commWeights[nbrCommId][commId] += nbr.weight;
                }
            }
        }
    }
    state.reset(newCommCount, mm, context);
    for (auto nodeId = 0u; nodeId < newCommCount; nodeId++) {
        state.initNextNode(nodeId);
        for (auto [nbrId, weight] : commWeights[nodeId]) {
            state.insertNbr(nodeId, nbrId, weight);
        }
    }
    state.finalize();
}

static common::offset_t tableFunc(const TableFuncInput& input, TableFuncOutput&) {
    auto clientContext = input.context->clientContext;
    auto transaction = clientContext->getTransaction();
    auto sharedState = input.sharedState->ptrCast<GDSFuncSharedState>();
    auto mm = clientContext->getMemoryManager();
    auto graph = sharedState->graph.get();
    auto maxOffsetMap = graph->getMaxOffsetMap(transaction);
    KU_ASSERT(graph->getNodeTableIDs().size() == 1);
    auto tableID = graph->getNodeTableIDs()[0];
    auto origNumNodes = graph->getMaxOffset(clientContext->getTransaction(), tableID);

    FinalResults finalResults(origNumNodes);
    PhaseState state(origNumNodes, mm, input.context);
    initInMemoryGraph(tableID, origNumNodes, graph, state);

    for (auto phase = 0u; phase < MAX_PHASES; ++phase) {
        double oldMod = -1;

        for (auto iter = 0u; iter < MAX_ITERATIONS; ++iter) {
            // Reset state.
            state.startNewIter(mm, input.context);

            // Move each node to a potentially new neighbor community that increases the modularity.
            RunIterationVC runIteration(state);
            InMemGDSUtils::runVertexCompute(runIteration, state.graph.numNodes, input.context);

            // Compute the modularity using the _current_ node communities, i.e., *before* the
            // node movements above. This keeps the logic for computing `sumIntraWeights` simpler.
            ComputeModularityVC::reset();
            ComputeModularityVC newModularityVC(state);
            InMemGDSUtils::runVertexCompute(newModularityVC, state.graph.numNodes, input.context);
            double currMod =
                ComputeModularityVC::sumIntraWeights.load() * state.modularityConstant -
                (ComputeModularityVC::sumWeightedDegrees.load() * state.modularityConstant *
                    state.modularityConstant);

            if (currMod - oldMod < THRESHOLD) {
                break;
            }
            oldMod = currMod;

            // nextCommInfo -> currCommInfo.
            UpdateCommInfosVC updateCommInfosVC(state);
            InMemGDSUtils::runVertexCompute(updateCommInfosVC, state.graph.numNodes, input.context);

            std::swap(state.previousComm, state.currComm);
            std::swap(state.currComm, state.nextComm);
        }
        auto oldCommCount = state.graph.numNodes;
        auto newCommCount = renumberCommunities(state);

        // Save the renumbered communities as output.
        SaveCommAssignmentsVC setFinalComms(phase, finalResults, state);
        InMemGDSUtils::runVertexCompute(setFinalComms, origNumNodes, input.context);

        if (oldCommCount == newCommCount) {
            break;
        }
        aggregateCommunities(newCommCount, state, mm, input.context);
    }

    auto vertexCompute = make_unique<WriteResultsVC>(mm, sharedState, finalResults);
    GDSUtils::runVertexCompute(input.context, GDSDensityState::DENSE, graph, *vertexCompute);

    sharedState->factorizedTablePool.mergeLocalTables();
    return 0;
}

static constexpr char GROUP_ID_COLUMN_NAME[] = "group_id";

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    auto graphName = input->getLiteralVal<std::string>(0);
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
    columns.push_back(input->binder->createVariable(GROUP_ID_COLUMN_NAME, LogicalType::INT64()));
    return std::make_unique<GDSBindData>(std::move(columns), std::move(graphEntry), nodeOutput);
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

} // namespace gds_extension
} // namespace kuzu
