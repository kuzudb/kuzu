#include <cmath>
#include <iostream>
#include <set>

#include "binder/binder.h"
#include "common/exception/runtime.h"
#include "degrees.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/gds_utils.h"
#include "gds_vertex_compute.h"
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
constexpr double WEIGHT = 1.0;
constexpr double THRESHOLD = 1e-6;
static constexpr offset_t OMAX = numeric_limits<offset_t>::max();

struct Neighbor {
    offset_t to;
    double weight;
};

struct InMemoryGraph {
    vector<offset_t> nodes;
    vector<Neighbor> neighbors;
    offset_t numNodes;
    offset_t numEdges = 0;

    explicit InMemoryGraph(const offset_t numNodes) : numNodes{numNodes} { init(numNodes); }
    // Delete copy constructors
    InMemoryGraph(const InMemoryGraph&) = delete;
    InMemoryGraph& operator=(const InMemoryGraph&) = delete;

    void init(const offset_t numNodes) {
        nodes.reserve(numNodes);
        nodes.clear();
        neighbors.clear();
        this->numNodes = numNodes;
        numEdges = 0;
    }

    void newNode() {
        // cout << std::format("new: {} -> {}\n", nodes.size(), neighbors.size());
        nodes.push_back(neighbors.size());
    }

    void addEdge(const offset_t to, const double weight = WEIGHT) {
        Neighbor neighbor{to, weight};
        neighbors.push_back(neighbor);
        numEdges++;
        // cout << std::format(" nbrs: {} -> {}\n", to, weight);
    }
};

struct CommInfo {
    long size;
    double degree;

    CommInfo() : size(0), degree(0.0) {}
};

struct PhaseState {
    InMemoryGraph graph;
    vector<double> nodeWeightedDegrees;
    vector<CommInfo> commInfos;
    vector<CommInfo> commInfosNext;
    vector<offset_t> pastCommunities;
    vector<offset_t> currCommunities;
    vector<offset_t> targetCommunities;
    vector<double> clusterWeightInternal;
    double totalWeight = 0.0;

    explicit PhaseState(const offset_t numNodes) : graph{InMemoryGraph(numNodes)} { init(numNodes); }
    // Delete copy constructors
    PhaseState(const PhaseState&) = delete;
    PhaseState& operator=(const PhaseState&) = delete;

    void init(const offset_t numNodes) {
        graph.init(numNodes);
        nodeWeightedDegrees.assign(numNodes, 0.0);
        commInfos.assign(numNodes, CommInfo());
        pastCommunities.assign(numNodes, OMAX);
        currCommunities.assign(numNodes, OMAX);
        targetCommunities.assign(numNodes, OMAX);
        totalWeight = 0.0;
    }

    void reset() {
        clusterWeightInternal.assign(graph.numNodes, 0.0);
        commInfosNext.assign(graph.numNodes, CommInfo());
    }

    void newNode(const offset_t nodeId) {
        graph.newNode();
        commInfos[nodeId].size = 1;
        commInfos[nodeId].degree = 0.0;
        pastCommunities[nodeId] = nodeId;
        currCommunities[nodeId] = nodeId;
    }

    void addEdge(const offset_t from, const offset_t to, const double weight = WEIGHT) {
        graph.addEdge(to, weight);
        nodeWeightedDegrees[from] += weight;
        commInfos[from].degree += weight;
        totalWeight += weight;
    }

    void finalize() {
        graph.newNode();
        // for (auto nodeId = 0u; nodeId < graph.numNodes; ++nodeId) {
        //     const auto from = graph.nodes[nodeId];
        //     const auto to = graph.nodes[nodeId + 1];
        //     cout << std::format("{}: {} {}\n", nodeId, from, to);
        //     for (auto offset = from; offset < to; ++offset) {
        //         cout << std::format(" {} {}\n", graph.neighbors[offset], graph.weights[offset]);
        //     }
        // }
    }
};

struct LouvainState {
    vector<offset_t> finalCommunities;

    explicit LouvainState(const offset_t numNodes) { finalCommunities.resize(numNodes); }
};

class LouvainCompute {
public:
    LouvainCompute(const table_id_t tableId, const offset_t numNodes, Graph* graph,
        LouvainState& louvainState)
        : state{PhaseState(numNodes)}, louvainState{louvainState}, origNumNodes{numNodes} {

        auto nbrTables = graph->getForwardNbrTableInfos(tableId);
        auto nbrInfo = nbrTables[0];
        auto scanState = graph->prepareRelScan(nbrInfo.relEntry, nbrInfo.nodeEntry, "");

        for (auto nodeId = 0u; nodeId < numNodes; ++nodeId) {
            state.newNode(nodeId);
            auto nextNodeId = nodeID_t{nodeId, tableId};
            for (auto chunk : graph->scanFwd(nextNodeId, *scanState.get())) {
                chunk.forEach([&](auto nbrNodeId, auto) {
                    auto nbrId = nbrNodeId.offset;
                    state.addEdge(nodeId, nbrId);
                });
            }
            for (auto chunk : graph->scanBwd(nextNodeId, *scanState.get())) {
                chunk.forEach([&](auto nbrNodeId, auto) {
                    auto nbrId = nbrNodeId.offset;
                    if (nbrId != nodeId) {
                        state.addEdge(nodeId, nbrId);
                    }
                });
            }
        }
        state.finalize();
    }
    // Delete copy constructors
    LouvainCompute(const LouvainCompute&) = delete;
    LouvainCompute& operator=(const LouvainCompute&) = delete;

    void compute() {
        for (auto phase = 0u; phase < MAX_LEVELS; ++phase) {
            auto oldCommCount = state.graph.numNodes;

            printf("Edges %ld\n", state.graph.numEdges);
            for (offset_t nodeId = 0u; nodeId < state.graph.numNodes; nodeId++) {
                auto from = state.graph.nodes[nodeId];
                auto to = state.graph.nodes[nodeId + 1];
                for (auto offset = from; offset < to; offset++) {
                    printf(" %ld-[%lf]->%ld\n", nodeId, state.graph.neighbors[offset].weight,
                        state.graph.neighbors[offset].to);
                }
            }

            runPhase();
            auto newCommCount = renumberCommunities();

            if (phase == 0) {
                // TODO: parallel NV
                for (auto nodeId = 0u; nodeId < origNumNodes; ++nodeId) {
                    louvainState.finalCommunities[nodeId] = state.pastCommunities[nodeId];
                }
            } else {
                // TODO: parallel NV
                for (auto nodeId = 0u; nodeId < origNumNodes; ++nodeId) {
                    auto prevCommunity = louvainState.finalCommunities[nodeId];
                    // Every previous community becomes a node in the current phase.
                    auto newCommunity = state.pastCommunities[prevCommunity];
                    louvainState.finalCommunities[nodeId] = newCommunity;
                }
            }

            if (oldCommCount == newCommCount) {
                break;
            }
            // cout << "Communities\n";
            // for (auto communityId = 0u; communityId < state.communities.size(); ++communityId) {
            //     string s;
            //     for (const auto nodeId : state.communities[communityId]) {
            //         s += std::format("{},", nodeId);
            //     }
            //     cout << std::format(" {} = [{}]\n", communityId, s);
            // }
            aggregateCommunities(newCommCount);
        }
    }

    void runPhase() {
        double oldMod = -1;
        double newMod = -1;
        double constantForSecondTerm = 1.0 / state.totalWeight;

        printf("====================================================\n");
        printf("Itr      E_xx            A_x2           Curr-Mod\n");
        printf("====================================================\n");

        for (auto iter = 0u; iter < MAX_ITERATIONS; ++iter) {
            state.reset();
            for (auto nodeId = 0u; nodeId < state.graph.numNodes; nodeId++) {
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
                            if (nbr.to == nodeId) {
                                lselfLoop += nbr.weight;
                            }
                            auto nbrCommId = state.currCommunities[nbr.to];
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
                    printf("  selfLoop = %u %lf\n", nodeId, selfLoop);
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
                            printf("    sc ? storedAlready: %lu %lu\n", sc, nbrCommId);
                            if (sc != nbrCommId) {
                                ay = state.commInfos[nbrCommId].degree;
                                eiy = counter[weight];
                                curGain = 2 * (eiy - eix) -
                                          2 * degree * (ay - ax) * constantForSecondTerm;
                                printf(
                                    "      degree ax ay constantForSecondTerm = %lf %lf %lf %lf\n",
                                    degree, ax, ay, constantForSecondTerm);
                                printf("      eix eiy currGain = %lf %lf %lf\n", eix, eiy, curGain);
                                if (curGain > maxGain || ((maxGain - curGain) < THRESHOLD && curGain != 0 &&
                                                             (nbrCommId < maxIndex))) {
                                    printf("        maxGain: %lf -> %lf\n", maxGain, curGain);
                                    printf("        maxIndex: %lu -> %lu\n", maxIndex, nbrCommId);
                                    maxGain = curGain;
                                    maxIndex = nbrCommId;
                                }
                            }
                        }
                        printf("    swap check: %lu %lu %lu %lu\n", state.commInfos[maxIndex].size, state.commInfos[sc].size, maxIndex, sc);
                        if (state.commInfos[maxIndex].size == 1 && state.commInfos[sc].size == 1 &&
                            maxIndex > sc) { // Swap protection
                            printf("      maxIndex: %lu -> %lu\n", maxIndex, sc);
                            maxIndex = sc;
                        }
                        max = maxIndex;
                        printf("  = targetCommAss[%u] = %ld\n", nodeId, max);
                    }
                    state.targetCommunities[nodeId] = max;
                } else {
                    state.targetCommunities[nodeId] = OMAX;
                }

                auto currCommId = state.currCommunities[nodeId];
                auto targetCommId = state.targetCommunities[nodeId];
                if (targetCommId != currCommId && targetCommId != OMAX) {
                    state.commInfosNext[targetCommId].degree += state.nodeWeightedDegrees[nodeId];
                    state.commInfosNext[targetCommId].size += 1;
                    printf("  : cUpdate[%lu] = %lf %lf %ld\n", targetCommId,
                        state.nodeWeightedDegrees[nodeId], state.commInfosNext[targetCommId].degree,
                        state.commInfosNext[targetCommId].size);
                    state.commInfosNext[currCommId].degree -= state.nodeWeightedDegrees[nodeId];
                    state.commInfosNext[currCommId].size -= 1;
                    printf("  : cUpdate[%lu] = %lf %ld\n", currCommId,
                        state.commInfosNext[currCommId].degree,
                        state.commInfosNext[currCommId].size);
                }
                clusterLocalMap.clear();
                counter.clear();
            }
            newMod = calculateModularity(iter);
            if (newMod - oldMod < THRESHOLD) {
                break;
            }
            oldMod = newMod;
            for (auto nodeId = 0u; nodeId < state.graph.numNodes; ++nodeId) {
                state.commInfos[nodeId].size += state.commInfosNext[nodeId].size;
                state.commInfos[nodeId].degree += state.commInfosNext[nodeId].degree;
            }
            std::swap(state.pastCommunities, state.currCommunities);
            std::swap(state.currCommunities, state.targetCommunities);
        }
    }

    offset_t renumberCommunities() {
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

    double calculateModularity(offset_t iter) {
        double sumIntra = 0.0;
        double sumTotal = 0.0;
        double constantForSecondTerm = 1.0 / state.totalWeight;
        for (auto nodeId = 0u; nodeId < state.graph.numNodes; ++nodeId) {
            sumIntra += state.clusterWeightInternal[nodeId];
            sumTotal += state.commInfos[nodeId].degree * state.commInfos[nodeId].degree;
        }
        auto newMod = sumIntra * constantForSecondTerm -
                      (sumTotal * constantForSecondTerm * constantForSecondTerm);
        printf("%lu \t %g \t %g \t %lf\n", iter, sumIntra, sumTotal, newMod);
        return newMod;
    }

    void aggregateCommunities(offset_t newCommCount) {
        vector<unordered_map<offset_t, double>> commWeights;
        commWeights.resize(newCommCount);
        for (auto nodeId = 0u; nodeId < state.graph.numNodes; nodeId++) {
            auto from = state.graph.nodes[nodeId];
            auto to = state.graph.nodes[nodeId + 1];
            auto commId = state.pastCommunities[nodeId];
            for (auto offset = from; offset < to; ++offset) {
                auto nbr = state.graph.neighbors[offset];
                auto nbrCommId = state.pastCommunities[nbr.to];
                if (commId >= nbrCommId) {
                    commWeights[commId][nbrCommId] += nbr.weight;
                    if (commId != nbrCommId) {
                        commWeights[nbrCommId][commId] += nbr.weight;
                    }
                }
            }
        }
        state.init(newCommCount);
        for (auto nodeId = 0u; nodeId < newCommCount; nodeId++) {
            state.newNode(nodeId);
            for (auto [nbrId, weight] : commWeights[nodeId]) {
                state.addEdge(nodeId, nbrId, weight);
            }
        }
        state.finalize();
    }

private:
    PhaseState state;
    LouvainState& louvainState;
    offset_t origNumNodes;
};

class SCCVertexCompute : public GDSResultVertexCompute {
public:
    SCCVertexCompute(MemoryManager* mm, GDSFuncSharedState* sharedState, LouvainState& louvainState)
        : GDSResultVertexCompute{mm, sharedState}, louvainState{louvainState} {
        nodeIDVector = createVector(LogicalType::INTERNAL_ID());
        componentIDVector = createVector(LogicalType::UINT64());
    }

    void beginOnTableInternal(table_id_t /*tableID*/) override {}

    void vertexCompute(const offset_t startOffset, const offset_t endOffset,
        const table_id_t tableID) override {
        for (auto i = startOffset; i < endOffset; ++i) {
            auto nodeID = nodeID_t{i, tableID};
            nodeIDVector->setValue<nodeID_t>(0, nodeID);
            componentIDVector->setValue<uint64_t>(0, louvainState.finalCommunities[i]);
            localFT->append(vectors);
        }
    }

    unique_ptr<VertexCompute> copy() override {
        return std::make_unique<SCCVertexCompute>(mm, sharedState, louvainState);
    }

private:
    LouvainState& louvainState;
    unique_ptr<ValueVector> nodeIDVector;
    unique_ptr<ValueVector> componentIDVector;
};

static common::offset_t tableFunc(const TableFuncInput& input, TableFuncOutput&) {
    auto clientContext = input.context->clientContext;
    auto transaction = clientContext->getTransaction();
    auto sharedState = input.sharedState->ptrCast<GDSFuncSharedState>();
    auto mm = clientContext->getMemoryManager();
    auto graph = sharedState->graph.get();
    auto maxOffsetMap = graph->getMaxOffsetMap(transaction);
    KU_ASSERT(graph->getNodeTableIDs().size() == 1);
    auto tableID = graph->getNodeTableIDs()[0];
    auto numNodes = graph->getMaxOffset(clientContext->getTransaction(), tableID);

    auto louvainState = LouvainState(numNodes);
    auto edgeCompute = make_unique<LouvainCompute>(tableID, numNodes, graph, louvainState);
    edgeCompute->compute();

    auto vertexCompute = make_unique<SCCVertexCompute>(mm, sharedState, louvainState);
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
        throw RuntimeException("Kosaraju's SCC only supports operations on one node table.");
    }
    if (graphEntry.relInfos.size() != 1) {
        throw RuntimeException("Kosaraju's SCC only supports operations on one edge table.");
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
