#include <cmath>

#include "binder/binder.h"
#include "common/exception/runtime.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/gds_utils.h"
#include "function/gds/gds_utils_mem.h"
#include "gds_vertex_compute.h"
#include "graph/graph_mem.h"
#include "processor/execution_context.h"

using namespace std;
using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::storage;
using namespace kuzu::graph;

// Louvain method for community detection: https://en.wikipedia.org/wiki/Louvain_method.
// For nodes in a graph grouped into `C` communities, the modularity of a graph is given as:
//   sumIntraWeights = sum over all c in C: sum of all edges in c, counted twice in both directions
//   sumWeightedDegrees = sum over all c in C: (sum of weighted degrees of all nodes in c)^2
//   modularity = sumIntraWeights/2m - sumWeightedDegrees/(2m)^2
// Parallel Louvain implementation following https://hpc.pnl.gov/people/hala/grappolo.html.

namespace kuzu {
namespace function {

constexpr uint32_t MAX_PHASES = 10;
constexpr uint32_t MAX_ITERATIONS = 10;
constexpr double THRESHOLD = 1e-6;
static constexpr offset_t UNASSIGNED_COMM = numeric_limits<offset_t>::max();

struct FinalResults {
    vector<offset_t> communities;

    explicit FinalResults(const offset_t numNodes) { communities.resize(numNodes); }
};

class CommInfo {
public:
    CommInfo() : size{0}, degree(0.0) {}
    CommInfo(const CommInfo& other) {
        size.store(other.getSize());
        degree.store(other.getDegree());
    }
    CommInfo& operator=(const CommInfo& other) {
        if (this != &other) {
            size.store(other.getSize());
            degree.store(other.getDegree());
        }
        return *this;
    }

    void addSize(long add) { size.fetch_add(add, memory_order_relaxed); }
    offset_t getSize() const { return size.load(memory_order_relaxed); }
    void setSize(const offset_t s) { size.store(s); }
    double getDegree() const { return degree.load(memory_order_relaxed); }
    void setDegree(const double d) { degree.store(d); }
    void addDegree(double add) { degree.fetch_add(add, memory_order_relaxed); }

private:
    std::atomic<offset_t> size;
    std::atomic<double> degree;
};

struct PhaseState {
    InMemGraph graph;
    AtomicObjectArray<double> nodeWeightedDegrees;
    ObjectArray<CommInfo> currCommInfos;
    ObjectArray<CommInfo> nextCommInfos;
    AtomicObjectArray<offset_t> currComm;
    AtomicObjectArray<offset_t> nextComm;
    AtomicObjectArray<double> clusterWeightInternal;
    double totalWeight = 0.0;        // Stores 2 * sum of edge weights.
    double modularityConstant = 0.0; // 1/2m.

    explicit PhaseState(const offset_t numNodes, MemoryManager* mm, ExecutionContext* context)
        : graph{InMemGraph(numNodes)} {
        reset(numNodes, mm, context);
    }
    DELETE_BOTH_COPY(PhaseState);

    void reset(offset_t numNodes, MemoryManager* mm, ExecutionContext* context);

    void startNewIter(MemoryManager* mm, ExecutionContext* context);

    void initNextNode(const offset_t nodeId) {
        graph.initNextNode();
        currCommInfos.getRef(nodeId).setSize(1);
        currCommInfos.getRef(nodeId).setDegree(0.0);
        currComm.setRelaxed(nodeId, nodeId);
    }

    void insertNbr(const offset_t from, const offset_t to, const double weight = DEFAULT_WEIGHT) {
        graph.insertNbr(to, weight);
        nodeWeightedDegrees.fetchAddRelaxed(from, weight);
        currCommInfos.getRef(from).addDegree(weight);
        totalWeight += weight;
    }

    void finalize() { graph.initNextNode(); }
};

class ResetPhaseStateVC : public InMemVertexCompute {
public:
    explicit ResetPhaseStateVC(PhaseState& state) : state{state} {}

    void vertexCompute(offset_t startOffset, offset_t endOffset) override {
        for (auto nodeId = startOffset; nodeId < endOffset; ++nodeId) {
            state.nodeWeightedDegrees.setRelaxed(nodeId, 0.0);
            state.currCommInfos.set(nodeId, CommInfo());
            state.currComm.setRelaxed(nodeId, UNASSIGNED_COMM);
            state.nextComm.setRelaxed(nodeId, UNASSIGNED_COMM);
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
            state.clusterWeightInternal.setRelaxed(nodeId, 0.0);
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
    totalWeight = 0.0;
    graph.reset(numNodes);
    nodeWeightedDegrees.resizeAsNew(numNodes, mm);
    currCommInfos.resizeAsNew(numNodes, mm);
    currComm.resizeAsNew(numNodes, mm);
    nextComm.resizeAsNew(numNodes, mm);

    ResetPhaseStateVC resetPhaseStateVC(*this);
    InMemGDSUtils::runVertexCompute(resetPhaseStateVC, numNodes, context);
}

void PhaseState::startNewIter(MemoryManager* mm, ExecutionContext* context) {
    clusterWeightInternal.resizeAsNew(graph.numNodes, mm);
    nextCommInfos.resizeAsNew(graph.numNodes, mm);

    StartNewIterVC startNewIterVC(*this);
    InMemGDSUtils::runVertexCompute(startNewIterVC, graph.numNodes, context);

    modularityConstant = 1.0 / totalWeight;
}

class SaveCommAssignmentsVC : public InMemVertexCompute {
public:
    explicit SaveCommAssignmentsVC(offset_t phaseId, FinalResults& finalResults, PhaseState& state)
        : phaseId{phaseId}, finalResults{finalResults}, state{state} {}

    void vertexCompute(offset_t startOffset, offset_t endOffset) override {
        if (phaseId == 0) {
            for (auto nodeId = startOffset; nodeId < endOffset; ++nodeId) {
                finalResults.communities[nodeId] = state.currComm.getRelaxed(nodeId);
            }
        } else {
            for (auto nodeId = startOffset; nodeId < endOffset; ++nodeId) {
                auto prevCommunity = finalResults.communities[nodeId];
                // Every previous community becomes a node in the current phase.
                auto newCommunity = state.currComm.getRelaxed(prevCommunity);
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
            double selfLoopWeight = 0.0;
            vector<double> intraCommWeights;
            unordered_map<offset_t, offset_t> commToWeightsIndex;
            if (startCSROffset != endCSROffset) {
                selfLoopWeight = computeIntraCommWeights(nodeId, startCSROffset, endCSROffset,
                    intraCommWeights, commToWeightsIndex);
                state.clusterWeightInternal.fetchAddRelaxed(nodeId, intraCommWeights[0]);
                offset_t newComm = findPotentialNewComm(nodeId, selfLoopWeight, intraCommWeights,
                    commToWeightsIndex);
                state.nextComm.setRelaxed(nodeId, newComm);
            } else {
                // Isolated node.
                state.nextComm.setRelaxed(nodeId, UNASSIGNED_COMM);
            }

            auto currCommId = state.currComm.getRelaxed(nodeId);
            auto targetCommId = state.nextComm.getRelaxed(nodeId);
            if (targetCommId != currCommId && targetCommId != UNASSIGNED_COMM) {
                state.nextCommInfos.getRef(targetCommId)
                    .addDegree(state.nodeWeightedDegrees.getRelaxed(nodeId));
                state.nextCommInfos.getRef(targetCommId).addSize(1);
                state.nextCommInfos.getRef(currCommId)
                    .addDegree(-state.nodeWeightedDegrees.getRelaxed(nodeId));
                state.nextCommInfos.getRef(currCommId).addSize(-1);
            }
            commToWeightsIndex.clear();
            intraCommWeights.clear();
        }
    }

    // Compute the intra community weights for each community `nodeId` can be moved to, including
    // its own.
    double computeIntraCommWeights(offset_t nodeId, offset_t startCSROffset, offset_t endCSROffset,
        vector<double>& intraCommWeights,
        unordered_map<offset_t, offset_t>& commToWeightsIndex) const {
        double selfLoopWeight = 0.0;
        commToWeightsIndex[state.currComm.getRelaxed(nodeId)] = 0; // current community.
        intraCommWeights.push_back(0);
        offset_t nextIndex = 1;
        for (auto offset = startCSROffset; offset < endCSROffset; offset++) {
            auto nbrEntry = state.graph.csrEdges[offset];
            if (nbrEntry.neighbor == nodeId) {
                selfLoopWeight += nbrEntry.weight;
            }
            auto nbrCommId = state.currComm.getRelaxed(nbrEntry.neighbor);
            if (!commToWeightsIndex.contains(nbrCommId)) {
                commToWeightsIndex[nbrCommId] = nextIndex;
                nextIndex++;
                intraCommWeights.push_back(nbrEntry.weight);
            } else {
                intraCommWeights[commToWeightsIndex[nbrCommId]] += nbrEntry.weight;
            }
        }
        return selfLoopWeight;
    }

    offset_t findPotentialNewComm(offset_t nodeId, double selfLoopWeight,
        vector<double> intraCommWeights,
        unordered_map<offset_t, offset_t> commToWeightsIndex) const {
        auto currComm = state.currComm.getRelaxed(nodeId);
        auto degree = state.nodeWeightedDegrees.getRelaxed(nodeId);
        auto moveToComm = currComm;
        auto moveToCommModGain = 0.0;
        auto prevIntraCommWeights = intraCommWeights[0] - selfLoopWeight;
        auto prevWeightedDegrees = state.currCommInfos.getRef(currComm).getDegree() - degree;
        for (auto [nbrCommId, weightIndex] : commToWeightsIndex) {
            if (currComm != nbrCommId) {
                auto newIntraCommWeights = intraCommWeights[weightIndex];
                auto newWeightedDegrees = state.currCommInfos.getRef(nbrCommId).getDegree();
                auto modGain = 2 * (newIntraCommWeights - prevIntraCommWeights) -
                               2 * degree * (newWeightedDegrees - prevWeightedDegrees) *
                                   state.modularityConstant;
                if (modGain > moveToCommModGain || ((moveToCommModGain - modGain) < THRESHOLD &&
                                                       modGain != 0 && (nbrCommId < moveToComm))) {
                    // Move if the gain is higher or if nbrComm has a lower ID with the same
                    // modularity.
                    moveToCommModGain = modGain;
                    moveToComm = nbrCommId;
                }
            }
        }
        if (state.currCommInfos.getRef(moveToComm).getSize() == 1 &&
            state.currCommInfos.getRef(currComm).getSize() == 1 &&
            moveToComm > currComm) { // Swap protection
            moveToComm = currComm;
        }
        return moveToComm;
    }

    std::unique_ptr<InMemVertexCompute> copy() override {
        return std::make_unique<RunIterationVC>(state);
    }

private:
    PhaseState& state;
};

class NewModularityVC : public InMemVertexCompute {
public:
    static std::atomic<double> sumIntraWeights;
    static std::atomic<double> sumWeightedDegrees;

    explicit NewModularityVC(PhaseState& state) : state{state} {}

    static void reset() {
        sumIntraWeights.store(0.0);
        sumWeightedDegrees.store(0.0);
    }

    void vertexCompute(offset_t startOffset, offset_t endOffset) override {
        double sumIntraLocal = 0.0;
        double sumTotalLocal = 0.0;
        for (auto nodeId = startOffset; nodeId < endOffset; ++nodeId) {
            sumIntraLocal += state.clusterWeightInternal.getRelaxed(nodeId);
            auto degree = state.currCommInfos.getRef(nodeId).getDegree();
            sumTotalLocal += degree * degree;
        }
        sumIntraWeights.fetch_add(sumIntraLocal);
        sumWeightedDegrees.fetch_add(sumTotalLocal);
    }

    std::unique_ptr<InMemVertexCompute> copy() override {
        return std::make_unique<NewModularityVC>(state);
    }

private:
    PhaseState& state;
};
std::atomic<double> NewModularityVC::sumIntraWeights{0.0};
std::atomic<double> NewModularityVC::sumWeightedDegrees{0.0};

class UpdateCommInfosVC : public InMemVertexCompute {
public:
    static std::atomic<double> sumIntra;
    static std::atomic<double> sumTotal;

    explicit UpdateCommInfosVC(PhaseState& state) : state{state} {}

    void vertexCompute(offset_t startOffset, offset_t endOffset) override {
        for (auto nodeId = startOffset; nodeId < endOffset; ++nodeId) {
            state.currCommInfos.getRef(nodeId).addSize(
                state.nextCommInfos.getRef(nodeId).getSize());
            state.currCommInfos.getRef(nodeId).addDegree(
                state.nextCommInfos.getRef(nodeId).getDegree());
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
    auto scanState = graph->prepareRelScan(nbrInfo.relEntry, nbrInfo.nodeEntry, "");

    for (auto nodeId = 0u; nodeId < numNodes; ++nodeId) {
        state.initNextNode(nodeId);
        auto nextNodeId = nodeID_t{nodeId, tableId};
        for (auto chunk : graph->scanFwd(nextNodeId, *scanState.get())) {
            chunk.forEach([&](auto nbrNodeId, auto) {
                auto nbrId = nbrNodeId.offset;
                state.insertNbr(nodeId, nbrId);
            });
        }
        for (auto chunk : graph->scanBwd(nextNodeId, *scanState.get())) {
            chunk.forEach([&](auto nbrNodeId, auto) {
                auto nbrId = nbrNodeId.offset;
                if (nbrId != nodeId) {
                    state.insertNbr(nodeId, nbrId);
                }
            });
        }
    }
    state.finalize();
}

offset_t renumberCommunities(PhaseState& state) {
    unordered_map<offset_t, offset_t> map;
    offset_t nextCommId = 0;
    offset_t nextUnassignedId = UNASSIGNED_COMM;
    for (auto i = 0u; i < state.currComm.getSize(); ++i) {
        auto commId = state.currComm.getRelaxed(i);
        // Assign unique IDs to unassigned nodes, otherwise they will merge into one community.
        if (commId == UNASSIGNED_COMM) {
            commId = nextUnassignedId;
            nextUnassignedId--;
        }
        if (!map.contains(commId)) {
            map.insert(make_pair(commId, nextCommId));
            nextCommId++;
        }
        state.currComm.setRelaxed(i, map.at(commId));
    }
    return nextCommId;
}

void aggregateCommunities(offset_t newCommCount, PhaseState& state, MemoryManager* mm,
    ExecutionContext* context) {
    vector<unordered_map<offset_t, double>> commWeights;
    commWeights.resize(newCommCount);
    for (auto nodeId = 0u; nodeId < state.graph.numNodes; nodeId++) {
        auto beginCSROffset = state.graph.csrOffsets[nodeId];
        auto endCSROffset = state.graph.csrOffsets[nodeId + 1];
        auto commId = state.currComm.getRelaxed(nodeId);
        for (auto offset = beginCSROffset; offset < endCSROffset; ++offset) {
            auto nbr = state.graph.csrEdges[offset];
            auto nbrCommId = state.currComm.getRelaxed(nbr.neighbor);
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

            // Compute the new modularity after all nodes movements, if any.
            NewModularityVC::reset();
            NewModularityVC newModularityVC(state);
            InMemGDSUtils::runVertexCompute(newModularityVC, state.graph.numNodes, input.context);
            double newMod = NewModularityVC::sumIntraWeights.load() * state.modularityConstant -
                            (NewModularityVC::sumWeightedDegrees.load() * state.modularityConstant *
                                state.modularityConstant);

            if (newMod - oldMod < THRESHOLD) {
                break;
            }
            oldMod = newMod;

            UpdateCommInfosVC updateCommInfosVC(state);
            InMemGDSUtils::runVertexCompute(updateCommInfosVC, state.graph.numNodes, input.context);

            std::swap(state.currComm, state.nextComm);
        }
        auto oldCommCount = state.graph.numNodes;
        auto newCommCount = renumberCommunities(state);

        SaveCommAssignmentsVC setFinalComms(phase, finalResults, state);
        InMemGDSUtils::runVertexCompute(setFinalComms, origNumNodes, input.context);

        if (oldCommCount == newCommCount) {
            break;
        }
        aggregateCommunities(newCommCount, state, mm, input.context);
    }

    auto vertexCompute = make_unique<WriteResultsVC>(mm, sharedState, finalResults);
    GDSUtils::runVertexCompute(input.context, graph, *vertexCompute);

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
    auto func = std::make_unique<TableFunction>(LouvainFunction::name,
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

} // namespace function
} // namespace kuzu
