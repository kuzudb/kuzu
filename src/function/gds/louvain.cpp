#include <cmath>
#include <set>

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

namespace kuzu {
namespace function {

constexpr uint32_t MAX_LEVELS = 10;
constexpr uint32_t MAX_ITERATIONS = 10;
constexpr double THRESHOLD = 1e-6;
static constexpr offset_t UNASSIGNED_COMM = numeric_limits<offset_t>::max();

struct CommInfo {
    long size;
    double degree;

    CommInfo() : size(0), degree(0.0) {}
};

class CommInfoAtomic {
public:
    CommInfoAtomic() : size{0}, degree(0.0) {}
    CommInfoAtomic(const CommInfoAtomic& other) {
        size.store(other.getSize());
        degree.store(other.getDegree());
    }
    CommInfoAtomic& operator=(const CommInfoAtomic& other) {
        if (this != &other) {
            size.store(other.getSize());
            degree.store(other.getDegree());
        }
        return *this;
    }

    void addSize(long add) { size.fetch_add(add, memory_order_relaxed); }
    offset_t getSize() const { return size.load(memory_order_relaxed); }
    double getDegree() const { return degree.load(memory_order_relaxed); }
    void addDegree(double add) { degree.fetch_add(add, memory_order_relaxed); }

private:
    std::atomic<offset_t> size;
    std::atomic<double> degree;
};

struct PhaseState {
    InMemoryGraph graph;
    vector<double> nodeWeightedDegrees;
    vector<CommInfo> commInfos;
    vector<CommInfoAtomic> commInfosNext;
    vector<offset_t> pastCommunities;
    vector<offset_t> currCommunities;
    vector<offset_t> targetCommunities;
    vector<double> clusterWeightInternal;
    double totalWeight = 0.0;

    explicit PhaseState(const offset_t numNodes) : graph{InMemoryGraph(numNodes)} {
        reset(numNodes);
    }
    DELETE_BOTH_COPY(PhaseState);

    void reset(const offset_t numNodes) {
        // Reuses allocations because `numNodes` monotonically decreases over phases.
        graph.reset(numNodes);
        nodeWeightedDegrees.assign(numNodes, 0.0);
        commInfos.assign(numNodes, CommInfo());
        pastCommunities.assign(numNodes, UNASSIGNED_COMM);
        currCommunities.assign(numNodes, UNASSIGNED_COMM);
        targetCommunities.assign(numNodes, UNASSIGNED_COMM);
        totalWeight = 0.0;
    }

    void startNewIter() {
        clusterWeightInternal.assign(graph.numNodes, 0.0);
        commInfosNext.assign(graph.numNodes, CommInfoAtomic());
    }

    void insertNewNode(const offset_t nodeId) {
        graph.insertNewNode();
        commInfos[nodeId].size = 1;
        commInfos[nodeId].degree = 0.0;
        pastCommunities[nodeId] = nodeId;
        currCommunities[nodeId] = nodeId;
    }

    void insertEdge(const offset_t from, const offset_t to, const double weight = DEFAULT_WEIGHT) {
        graph.insertEdge(to, weight);
        nodeWeightedDegrees[from] += weight;
        commInfos[from].degree += weight;
        totalWeight += weight;
    }

    void finalize() { graph.insertNewNode(); }
};

struct FinalResults {
    vector<offset_t> communities;

    explicit FinalResults(const offset_t numNodes) { communities.resize(numNodes); }
};

class SaveCommAssignments : public InMemVertexCompute {
public:
    explicit SaveCommAssignments(offset_t phaseId, FinalResults& finalResults, PhaseState& state)
        : phaseId{phaseId}, finalResults{finalResults}, state{state} {}

    void vertexCompute(offset_t startOffset, offset_t endOffset) override {
        if (phaseId == 0) {
            for (auto nodeId = startOffset; nodeId < endOffset; ++nodeId) {
                finalResults.communities[nodeId] = state.pastCommunities[nodeId];
            }
        } else {
            for (auto nodeId = startOffset; nodeId < endOffset; ++nodeId) {
                auto prevCommunity = finalResults.communities[nodeId];
                // Every previous community becomes a node in the current phase.
                auto newCommunity = state.pastCommunities[prevCommunity];
                finalResults.communities[nodeId] = newCommunity;
            }
        }
    }

    std::unique_ptr<InMemVertexCompute> copy() override {
        return std::make_unique<SaveCommAssignments>(phaseId, finalResults, state);
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
        double constantForSecondTerm = 1.0 / state.totalWeight;
        for (auto nodeId = startOffset; nodeId < endOffset; ++nodeId) {
            auto from = state.graph.nodes[nodeId];
            auto to = state.graph.nodes[nodeId + 1];
            double selfLoop = 0.0;
            unordered_map<offset_t, offset_t> clusterLocalMap;
            vector<double> counter;
            if (from != to) {
                clusterLocalMap[state.currCommunities[nodeId]] = 0;
                counter.push_back(0);
                {
                    double lselfLoop = 0.0;
                    offset_t numUniqueClusters = 1;
                    for (auto offset = from; offset < to; offset++) {
                        auto nbr = state.graph.neighbors[offset];
                        if (nbr.neighbor == nodeId) {
                            lselfLoop += nbr.weight;
                        }
                        auto nbrCommId = state.currCommunities[nbr.neighbor];
                        if (!clusterLocalMap.contains(nbrCommId)) {
                            clusterLocalMap[nbrCommId] = numUniqueClusters;
                            numUniqueClusters++;
                            counter.push_back(nbr.weight);
                        } else {
                            counter[clusterLocalMap[nbrCommId]] += nbr.weight;
                        }
                    }
                    selfLoop = lselfLoop;
                }
                state.clusterWeightInternal[nodeId] += counter[0];
                offset_t max = 0;
                {
                    offset_t sc = state.currCommunities[nodeId];
                    double degree = state.nodeWeightedDegrees[nodeId];
                    offset_t maxIndex = sc; // Assign the initial value as self community
                    double curGain = 0;
                    double maxGain = 0;
                    double eix = counter[0] - selfLoop;
                    double ax = state.commInfos[sc].degree - degree;
                    double eiy = 0;
                    double ay = 0;
                    for (auto [nbrCommId, weight] : clusterLocalMap) {
                        if (sc != nbrCommId) {
                            ay = state.commInfos[nbrCommId].degree;
                            eiy = counter[weight];
                            curGain =
                                2 * (eiy - eix) - 2 * degree * (ay - ax) * constantForSecondTerm;
                            if (curGain > maxGain || ((maxGain - curGain) < THRESHOLD &&
                                                         curGain != 0 && (nbrCommId < maxIndex))) {
                                maxGain = curGain;
                                maxIndex = nbrCommId;
                            }
                        }
                    }
                    if (state.commInfos[maxIndex].size == 1 && state.commInfos[sc].size == 1 &&
                        maxIndex > sc) { // Swap protection
                        maxIndex = sc;
                    }
                    max = maxIndex;
                }
                state.targetCommunities[nodeId] = max;
            } else {
                state.targetCommunities[nodeId] = UNASSIGNED_COMM;
            }

            auto currCommId = state.currCommunities[nodeId];
            auto targetCommId = state.targetCommunities[nodeId];
            if (targetCommId != currCommId && targetCommId != UNASSIGNED_COMM) {
                state.commInfosNext[targetCommId].addDegree(state.nodeWeightedDegrees[nodeId]);
                state.commInfosNext[targetCommId].addSize(1);
                state.commInfosNext[currCommId].addDegree(-state.nodeWeightedDegrees[nodeId]);
                state.commInfosNext[currCommId].addSize(-1);
            }
            clusterLocalMap.clear();
            counter.clear();
        }
    }

    std::unique_ptr<InMemVertexCompute> copy() override {
        return std::make_unique<RunIterationVC>(state);
    }

private:
    PhaseState& state;
};

class NewModularityVC : public InMemVertexCompute {
public:
    static std::atomic<double> sumIntra;
    static std::atomic<double> sumTotal;

    explicit NewModularityVC(PhaseState& state) : state{state} {}

    void vertexCompute(offset_t startOffset, offset_t endOffset) override {
        double sumIntraLocal = 0.0;
        double sumTotalLocal = 0.0;
        for (auto nodeId = startOffset; nodeId < endOffset; ++nodeId) {
            sumIntraLocal += state.clusterWeightInternal[nodeId];
            sumTotalLocal += state.commInfos[nodeId].degree * state.commInfos[nodeId].degree;
        }
        sumIntra.fetch_add(sumIntraLocal);
        sumTotal.fetch_add(sumTotalLocal);
    }

    std::unique_ptr<InMemVertexCompute> copy() override {
        return std::make_unique<NewModularityVC>(state);
    }

private:
    PhaseState& state;
};
std::atomic<double> NewModularityVC::sumIntra{0.0};
std::atomic<double> NewModularityVC::sumTotal{0.0};

class UpdateCommInfosVC : public InMemVertexCompute {
public:
    static std::atomic<double> sumIntra;
    static std::atomic<double> sumTotal;

    explicit UpdateCommInfosVC(PhaseState& state) : state{state} {}

    void vertexCompute(offset_t startOffset, offset_t endOffset) override {
        for (auto nodeId = startOffset; nodeId < endOffset; ++nodeId) {
            state.commInfos[nodeId].size += state.commInfosNext[nodeId].getSize();
            state.commInfos[nodeId].degree += state.commInfosNext[nodeId].getDegree();
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
        state.insertNewNode(nodeId);
        auto nextNodeId = nodeID_t{nodeId, tableId};
        for (auto chunk : graph->scanFwd(nextNodeId, *scanState.get())) {
            chunk.forEach([&](auto nbrNodeId, auto) {
                auto nbrId = nbrNodeId.offset;
                state.insertEdge(nodeId, nbrId);
            });
        }
        for (auto chunk : graph->scanBwd(nextNodeId, *scanState.get())) {
            chunk.forEach([&](auto nbrNodeId, auto) {
                auto nbrId = nbrNodeId.offset;
                if (nbrId != nodeId) {
                    state.insertEdge(nodeId, nbrId);
                }
            });
        }
    }
    state.finalize();
}

offset_t renumberCommunities(PhaseState& state) {
    unordered_map<offset_t, offset_t> map;
    offset_t nextCommId = 0;
    for (auto i = 0u; i < state.pastCommunities.size(); ++i) {
        auto commId = state.pastCommunities[i];
        if (!map.contains(commId)) {
            map.insert(make_pair(commId, nextCommId));
            nextCommId++;
        }
        state.pastCommunities[i] = map.at(commId);
    }
    return nextCommId;
}

void aggregateCommunities(offset_t newCommCount, PhaseState& state) {
    vector<unordered_map<offset_t, double>> commWeights;
    commWeights.resize(newCommCount);
    for (auto nodeId = 0u; nodeId < state.graph.numNodes; nodeId++) {
        auto from = state.graph.nodes[nodeId];
        auto to = state.graph.nodes[nodeId + 1];
        auto commId = state.pastCommunities[nodeId];
        for (auto offset = from; offset < to; ++offset) {
            auto nbr = state.graph.neighbors[offset];
            auto nbrCommId = state.pastCommunities[nbr.neighbor];
            if (commId >= nbrCommId) {
                commWeights[commId][nbrCommId] += nbr.weight;
                if (commId != nbrCommId) {
                    commWeights[nbrCommId][commId] += nbr.weight;
                }
            }
        }
    }
    state.reset(newCommCount);
    for (auto nodeId = 0u; nodeId < newCommCount; nodeId++) {
        state.insertNewNode(nodeId);
        for (auto [nbrId, weight] : commWeights[nodeId]) {
            state.insertEdge(nodeId, nbrId, weight);
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
    PhaseState state(origNumNodes);
    initInMemoryGraph(tableID, origNumNodes, graph, state);

    for (auto phase = 0u; phase < MAX_LEVELS; ++phase) {
        double oldMod = -1;

        for (auto iter = 0u; iter < MAX_ITERATIONS; ++iter) {
            state.startNewIter();

            RunIterationVC runIteration(state);
            GDSUtilsInMemory::runVertexCompute(runIteration, state.graph.numNodes, input.context);

            NewModularityVC newModularityVC(state);
            GDSUtilsInMemory::runVertexCompute(newModularityVC, state.graph.numNodes,
                input.context);
            double constantForSecondTerm = 1.0 / state.totalWeight;
            double newMod =
                newModularityVC.sumIntra.load() * constantForSecondTerm -
                (newModularityVC.sumTotal.load() * constantForSecondTerm * constantForSecondTerm);

            if (newMod - oldMod < THRESHOLD) {
                break;
            }
            oldMod = newMod;

            UpdateCommInfosVC updateCommInfosVC(state);
            GDSUtilsInMemory::runVertexCompute(updateCommInfosVC, state.graph.numNodes,
                input.context);

            std::swap(state.pastCommunities, state.currCommunities);
            std::swap(state.currCommunities, state.targetCommunities);
        }
        auto oldCommCount = state.graph.numNodes;
        auto newCommCount = renumberCommunities(state);

        SaveCommAssignments setFinalComms(phase, finalResults, state);
        GDSUtilsInMemory::runVertexCompute(setFinalComms, origNumNodes, input.context);

        if (oldCommCount == newCommCount) {
            break;
        }
        aggregateCommunities(newCommCount, state);
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
