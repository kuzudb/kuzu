#include <atomic>
#include <cassert>
#include <cstdint>
#include <iostream>
#include <memory>
#include <optional>
#include <thread>
#include <tuple>
#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "common/exception/binder.h"
#include "common/exception/runtime.h"
#include "common/in_mem_gds_utils.h"
#include "common/in_mem_graph.h"
#include "common/string_utils.h"
#include "common/types/types.h"
#include "function/algo_function.h"
#include "function/gds/gds.h"
#include "function/gds/gds_utils.h"
#include "function/gds/gds_vertex_compute.h"
#include "function/table/bind_input.h"
#include "processor/execution_context.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::storage;
using namespace kuzu::graph;
using namespace kuzu::function;
using Edge = std::tuple<uint64_t, uint64_t, uint64_t>;
using Edges = std::vector<Edge>;

static constexpr uint64_t U = 0; 
static constexpr uint64_t V = 1;
static constexpr uint64_t WEIGHT = 2;

namespace kuzu {
namespace algo_extension {


class WriteResultsMSF final : public GDSResultVertexCompute {
public:
    WriteResultsMSF(MemoryManager* mm, GDSFuncSharedState* sharedState, std::vector<std::vector<std::pair<offset_t, offset_t>>>& final)
        : GDSResultVertexCompute{mm, sharedState}, finalResults{final} {
        nodeIDVector = createVector(LogicalType::INTERNAL_ID());
        toIDVector = createVector(LogicalType::INTERNAL_ID());
        weightVector = createVector(LogicalType::INT64());
    }

    void beginOnTableInternal(table_id_t /*tableID*/) override {}

    void vertexCompute(const offset_t startOffset, const offset_t endOffset, const table_id_t tableID) override {
        for(auto i = startOffset; i < endOffset; ++i)
        {
            for(auto j = 0u; j < finalResults[i].size(); ++j)
            {
                const auto nodeID = nodeID_t{i, tableID};
                nodeIDVector->setValue<nodeID_t>(0, nodeID);
                toIDVector->setValue<nodeID_t>(0, nodeID_t{finalResults[i][j].first, tableID});
                weightVector->setValue<offset_t>(0, finalResults[i][j].second);
                localFT->append(vectors);
            }
        }
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<WriteResultsMSF>(mm, sharedState, finalResults);
    }

private:
    std::vector<std::vector<std::pair<offset_t, offset_t>>>& finalResults;
    std::unique_ptr<ValueVector> nodeIDVector;
    std::unique_ptr<ValueVector> toIDVector;
    std::unique_ptr<ValueVector> weightVector;
};

struct MSFConfig final : public GDSConfig {
    std::string weight_property;
    MSFConfig() = default;
};


struct MSFOptionalParams final : public GDSOptionalParams {
    std::shared_ptr<Expression> weightProperty;

    explicit MSFOptionalParams(const expression_vector& optionalParams);

    std::unique_ptr<GDSConfig> getConfig() const override;

    std::unique_ptr<GDSOptionalParams> copy() const override {
        return std::make_unique<MSFOptionalParams>(*this);
    }
};

MSFOptionalParams::MSFOptionalParams(const expression_vector& optionalParams)
{
    for (auto& optionalParam : optionalParams) {
        auto paramName = StringUtils::getLower(optionalParam->getAlias());
        if (paramName == "property") {
            weightProperty = optionalParam;
        } else {
            throw BinderException{"Unknown optional parameter: " + optionalParam->getAlias()};
        }
    }
}

std::unique_ptr<GDSConfig> MSFOptionalParams::getConfig() const {
    auto config = std::make_unique<MSFConfig>();
    if (weightProperty == nullptr)
    {
        throw RuntimeException("No parameter specifying the weight\n");
    }
    config->weight_property = ExpressionUtil::evaluateLiteral<std::string>(*weightProperty, LogicalType::STRING());

    return config;
}


// we may be able to refactor this with componentID?
static uint64_t find(std::vector<std::atomic<uint64_t>>& parent, uint64_t u) 
{
    while (true) 
    {
        uint64_t p = parent[u].load(std::memory_order::acquire);
        uint64_t gp = parent[p].load(std::memory_order::acquire);
        if (p == gp)
        {
            return p;
        }
        parent[u].compare_exchange_weak(p, gp, std::memory_order::acq_rel);
        u = p;
    }
}

// we may be able to refactor this with componentID?
static bool tryUnion(std::vector<std::atomic<uint64_t>>& parent, uint64_t u, uint64_t v)
{
    while (true)
    {
        u = find(parent, u);
        v = find(parent, v);
        if (u == v) 
        {
            // Nodes already in same component
            return false;
        }

        if (u < v) 
        {
            std::swap(u, v);
        }

        if (parent[u].compare_exchange_strong(u, v, std::memory_order::acq_rel))
        {
            return true;
        }

        // Failed, retry
    }
}

//static void filterEdges(Edges& edges, std::vector<std::atomic<uint64_t>>& parent)
//{
//    auto numThreads = std::thread::hardware_concurrency();
//    auto chunkSize = (edges.size() + numThreads - 1) / numThreads;
//    if (chunkSize < 10000)
//    {
//        edges.erase
//        (
//            std::remove_if(edges.begin(), edges.end(), 
//            [&](auto& e) {return find(parent, std::get<U>(e)) == find(parent, std::get<V>(e));}
//            ),
//            edges.end()
//        );
//        return;
//    }
//    std::vector<size_t> newSizes(numThreads);
//    std::vector<std::thread> threads;
//    for (auto t = 0u; t < numThreads; ++t) 
//    {
//        auto start = t * chunkSize;
//        auto end = std::min(edges.size(), start + chunkSize);
//
//        threads.emplace_back([&, t, start, end]()
//        {
//            auto writeIt = edges.begin() + start;
//            for (auto it = edges.begin() + start; it != edges.begin() + end; ++it) 
//            {
//                if (find(parent, std::get<U>(*it)) != find(parent, std::get<V>(*it)))
//                {
//                    *writeIt = std::move(*it);
//                    ++writeIt;
//                }
//            }
//            newSizes[t] = std::distance(edges.begin() + start, writeIt);
//        });
//    }
//    for(auto& th : threads)
//    {
//        th.join();
//    }
//    auto offset = 0u;
//    for (auto t = 0u; t < numThreads; ++t) 
//    {
//        auto start = t * chunkSize;
//        auto size = newSizes[t];
//        if (start != offset) 
//        {
//            std::move(edges.begin() + start, edges.begin() + start + size, edges.begin() + offset);
//        }
//        offset += size;
//    }
//    edges.resize(offset);
//}

//static void assignCheapestEdges(Edges& edges, std::vector<std::atomic<uint64_t>>& parent, std::vector<std::optional<Edge>>& cheapest)
//{
//    auto numThreads = std::thread::hardware_concurrency();
//    auto chunkSize = (edges.size() + numThreads - 1) / numThreads;
//    if (chunkSize < 10000)
//    {
//        for(const auto& e : edges)
//        {
//            auto u_comp = find(parent, std::get<U>(e));
//            auto v_comp = find(parent, std::get<V>(e));
//            // Update each component with the min edge found so far.
//            auto& cu = cheapest[u_comp];
//            if (cu == std::nullopt || std::get<WEIGHT>(e) < std::get<WEIGHT>(cu.value()))
//            {
//                cheapest[u_comp] = e;
//            }
//            auto& cv = cheapest[v_comp];
//            if (cv == std::nullopt|| std::get<WEIGHT>(e) < std::get<WEIGHT>(cv.value()))
//            {
//                cheapest[v_comp] = e;
//            }
//        }
//        return;
//    }
//    std::vector<std::thread> threads;
//    static std::vector<std::mutex> locks(cheapest.size());
//
//    for (auto t = 0u; t < numThreads; ++t) 
//    {
//        auto start = t * chunkSize;
//        auto end = std::min(edges.size(), start + chunkSize);
//
//        threads.emplace_back([&, start, end]()
//        {
//            for (auto it = edges.begin() + start; it != edges.begin() + end; ++it) 
//            {
//                auto u_comp = find(parent, std::get<U>(*it));
//                auto v_comp = find(parent, std::get<V>(*it));
//                auto tryUpdate = [&](uint64_t comp, const Edge& candidate) 
//                {
//                    std::lock_guard<std::mutex> guard(locks[comp]);
//                    auto& curr = cheapest[comp];
//                    if (curr == std::nullopt || std::get<WEIGHT>(candidate) < std::get<WEIGHT>(curr.value()))
//                    {
//                        curr = candidate;
//                    }
//                };
//                tryUpdate(u_comp, *it);
//                tryUpdate(v_comp, *it);
//            }
//        });
//    }
//    for(auto& th : threads)
//    {
//        th.join();
//    }
//}


static bool updateForest(std::vector<std::vector<std::pair<offset_t, offset_t>>>& finalEdges, std::vector<std::atomic<uint64_t>>& parent, std::vector<std::optional<Edge>>& cheapest)
{
    auto numThreads = std::thread::hardware_concurrency();
    auto chunkSize = (cheapest.size() + numThreads - 1) / numThreads;
    // should have to do with density
    if (chunkSize < 10000)
    {
        bool addedEdge = false;
        for(auto& e : cheapest)
        {
            // Skip all invalid components (i.e no outgoing edges or not the
            // parent of the component).
            if (e == std::nullopt)
            {
                continue;
            }
            // We must do this check again... if not done an earlier iteration of the loop
            // may have added an edge such that a later iteration attempts to
            // add an edge that causes a cycle.
            // DSU to mark two components as joined.
            if (tryUnion(parent, std::get<U>(e.value()), std::get<V>(e.value())))
            {
                finalEdges[std::get<U>(e.value())].push_back({std::get<V>(e.value()), std::get<WEIGHT>(e.value())});
                finalEdges[std::get<V>(e.value())].push_back({std::get<U>(e.value()), std::get<WEIGHT>(e.value())});
                addedEdge = true;
            }
            // Need to do this since we expect cheapest container to be reset
            // for next round.
            e = std::nullopt;
        }
        return addedEdge;
    }
    std::atomic<bool> addedEdge{false};
    std::vector<std::thread> threads;
    std::vector<Edges> localEdges(numThreads);
    for (auto t = 0u; t < numThreads; ++t) 
    {
        auto start = t * chunkSize;
        auto end = std::min(cheapest.size(), start + chunkSize);

        threads.emplace_back([&, t, start, end]()
        {
            for (auto it = cheapest.begin() + start; it != cheapest.begin() + end; ++it) 
            {
                if (*it == std::nullopt)
                {
                    continue;
                }
                if (tryUnion(parent, std::get<U>(it->value()), std::get<V>(it->value())))
                {
                    localEdges[t].push_back(it->value());
                    addedEdge.store(true, std::memory_order::relaxed);
                }
                *it = std::nullopt;
            }
        });
    }
    for(auto& th : threads)
    {
        th.join();
    }
    for (auto t = 0u; t < numThreads; ++t) 
    {
        for (auto& e : localEdges[t]) 
        {
            finalEdges[std::get<U>(e)].push_back({std::get<V>(e), std::get<WEIGHT>(e)});
            finalEdges[std::get<V>(e)].push_back({std::get<U>(e), std::get<WEIGHT>(e)});
        }
    }
    return addedEdge.load(std::memory_order::relaxed);
}

struct BoruvkaState 
{
    InMemGraph& graph;
    std::vector<std::atomic<uint64_t>> parent;
    std::vector<std::optional<Edge>> cheapest;
    std::vector<std::vector<std::pair<offset_t, offset_t>>> finalEdges;
    std::vector<std::mutex> locks;
    std::vector<std::atomic<bool>> validEdges;
    explicit BoruvkaState(offset_t numNodes, InMemGraph& graph) : graph{graph}, parent(numNodes), cheapest(numNodes, std::nullopt), finalEdges(numNodes), locks(numNodes), validEdges(graph.numEdges)
    {
        for(auto i = 0u; i < parent.size(); ++i) 
        {
            parent[i].store(i, std::memory_order::relaxed);
        }

        for(auto i = 0u; i < validEdges.size(); ++i) 
        {
            validEdges[i].store(true, std::memory_order::relaxed);
        }

    }
};

class AssignCheapestEdgesCompute final : public InMemVertexCompute {
public:
    explicit AssignCheapestEdgesCompute(BoruvkaState& state) : state{state} {}
    ~AssignCheapestEdgesCompute() override = default;
    void vertexCompute(const offset_t startOffset, const offset_t endOffset) override {
        for (auto nodeId = startOffset; nodeId < endOffset; ++nodeId) {
            const auto startCSROffset = state.graph.csrOffsets[nodeId];
            const auto endCSROffset = state.graph.csrOffsets[nodeId + 1];
            for (auto offset = startCSROffset; offset < endCSROffset; offset++)
            {
                if (!state.validEdges[offset].load(std::memory_order::relaxed))
                {
                    continue;
                }
                auto u_comp = find(state.parent, nodeId);
                auto v_comp = find(state.parent, state.graph.csrEdges[offset].neighbor);
                auto tryUpdate = [&](uint64_t comp, const Edge& candidate) 
                {
                    std::lock_guard<std::mutex> guard(state.locks[comp]);
                    auto& curr = state.cheapest[comp];
                    if (curr == std::nullopt || std::get<WEIGHT>(candidate) < std::get<WEIGHT>(curr.value()))
                    {
                        curr = candidate;
                    }
                };
                Edge edge = {nodeId, state.graph.csrEdges[offset].neighbor, state.graph.csrEdges[offset].weight};
                tryUpdate(u_comp, edge);
                tryUpdate(v_comp, edge);
            }
        }
    }
    std::unique_ptr<InMemVertexCompute> copy() override {
        return std::make_unique<AssignCheapestEdgesCompute>(state);
    }
private:
    BoruvkaState& state;
};



class UpdateForestCompute final : public InMemVertexCompute {
public:
    explicit UpdateForestCompute(BoruvkaState& state) : state{state} {}
    ~UpdateForestCompute() override = default;
    void vertexCompute(const offset_t startOffset, const offset_t endOffset) override {
        for (auto nodeId = startOffset; nodeId < endOffset; ++nodeId) {
            const auto startCSROffset = state.graph.csrOffsets[nodeId];
            const auto endCSROffset = state.graph.csrOffsets[nodeId + 1];
            for (auto offset = startCSROffset; offset < endCSROffset; offset++)
            {
            }
        }
    }
    std::unique_ptr<InMemVertexCompute> copy() override {
        return std::make_unique<UpdateForestCompute>(state);
    }
private:
    BoruvkaState& state;
};


class FilterEdgesCompute final : public InMemVertexCompute {
public:
    explicit FilterEdgesCompute(BoruvkaState& state) : state{state} {}
    ~FilterEdgesCompute() override = default;
    void vertexCompute(const offset_t startOffset, const offset_t endOffset) override {
        for (auto nodeId = startOffset; nodeId < endOffset; ++nodeId) {
            const auto startCSROffset = state.graph.csrOffsets[nodeId];
            const auto endCSROffset = state.graph.csrOffsets[nodeId + 1];
            for (auto offset = startCSROffset; offset < endCSROffset; offset++)
            {
                if (!state.validEdges[offset].load(std::memory_order::relaxed))
                {
                    continue;
                }

                if (find(state.parent, nodeId) == find(state.parent, state.graph.csrEdges[offset].neighbor))
                {
                    state.validEdges[offset].store(false, std::memory_order::relaxed);
                }
            }
        }
    }
    std::unique_ptr<InMemVertexCompute> copy() override {
        return std::make_unique<FilterEdgesCompute>(state);
    }
private:
    BoruvkaState& state;
};



static offset_t tableFunc(const TableFuncInput& input, TableFuncOutput&) {
    const auto clientContext = input.context->clientContext;
    auto sharedState = input.sharedState->ptrCast<GDSFuncSharedState>();
    auto graph = sharedState->graph.get();
    KU_ASSERT(graph->getNodeTableIDs().size() == 1);
    const auto tableId = graph->getNodeTableIDs()[0];
    auto mm = clientContext->getMemoryManager();
    const auto nbrTables = graph->getRelInfos(tableId);
    const auto nbrInfo = nbrTables[0];
    KU_ASSERT(nbrInfo.srcTableID == nbrInfo.dstTableID);
    auto MSFBindData = input.bindData->constPtrCast<GDSBindData>();
    auto config = MSFBindData->getConfig()->constCast<MSFConfig>();
    if (!nbrTables[0].relGroupEntry->containsProperty(config.weight_property))
    {
        throw RuntimeException("Cannot find property " + config.weight_property);
    }
    const auto scanState = graph->prepareRelScan(*nbrInfo.relGroupEntry, nbrInfo.relTableID, nbrInfo.dstTableID, {config.weight_property});
    const auto numNodes = graph->getMaxOffset(clientContext->getTransaction(), tableId);

    InMemGraph g(numNodes, mm);
    for (auto nodeId = 0u; nodeId < numNodes; ++nodeId) {
        g.initNextNode();
        const nodeID_t nextNodeId = {nodeId, tableId};
        for (auto chunk : graph->scanFwd(nextNodeId, *scanState)) {
            chunk.forEach([&](auto neighbors, auto propertyVectors, auto i) {
                auto nbrId = neighbors[i].offset;
                auto weight = propertyVectors[0]->template getValue<int64_t>(i);
                g.insertNbr(nbrId, weight);
            });
        }
    }
    // Finalize
    g.initNextNode();

    BoruvkaState state(numNodes, g);
    AssignCheapestEdgesCompute assignCheapestEdges(state);
    FilterEdgesCompute filterEdges(state);

    //TODO(Tanvir) Inquire about maxIterations
    int count = 0;
    while(++count)
    {
        // Loop over all edges finding the cheapest outgoing edge on a
        // component.
        InMemGDSUtils::runVertexCompute(assignCheapestEdges, numNodes, input.context);
        // Add the cheapest edge from each component to the forest. 
        // This also updates our component container (parent).
        // We break if we make no progress on a round.
        bool addedEdge = updateForest(state.finalEdges, state.parent, state.cheapest);
        if (!addedEdge)
        {
            break;
        }
        
        // Optimization: Erase edges in the same component from candidates.
        InMemGDSUtils::runVertexCompute(filterEdges, numNodes, input.context);
    }

    const auto vertexCompute = make_unique<WriteResultsMSF>(mm, sharedState, state.finalEdges);
    GDSUtils::runVertexCompute(input.context, GDSDensityState::DENSE, graph, *vertexCompute);

    sharedState->factorizedTablePool.mergeLocalTables();

    return 0;
}

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    auto graphName = input->getLiteralVal<std::string>(0);
    auto graphEntry = GDSFunction::bindGraphEntry(*context, graphName);
    //TODO (TANVIR) Attempt to remove this restriction
    if (graphEntry.nodeInfos.size() != 1) {
        throw BinderException(std::string(MinimumSpanningForest::name) + " only supports operations on one node table.");
    }
    if (graphEntry.relInfos.size() != 1) {
        throw BinderException(std::string(MinimumSpanningForest::name) + " only supports operations on one edge table.");
    }
    auto nodeOutput = GDSFunction::bindNodeOutput(*input, graphEntry.getNodeEntries());
    expression_vector columns;
    columns.push_back(nodeOutput->constCast<NodeExpression>().getInternalID());
    columns.push_back(input->binder->createVariable("TO", LogicalType::INTERNAL_ID()));
    columns.push_back(input->binder->createVariable("WEIGHT", LogicalType::INT64()));
    return std::make_unique<GDSBindData>(std::move(columns), std::move(graphEntry), nodeOutput, std::make_unique<MSFOptionalParams>(input->optionalParamsLegacy));
}

function_set MinimumSpanningForest::getFunctionSet() {
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
