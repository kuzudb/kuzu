#include <cstdint>
#include <iostream>
#include <memory>
#include <optional>
#include <tuple>
#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "common/exception/binder.h"
#include "common/exception/runtime.h"
#include "common/string_utils.h"
#include "common/types/types.h"
#include "function/algo_function.h"
#include "function/gds/gds.h"
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
namespace kuzu {
namespace algo_extension {


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
    config->weight_property = ExpressionUtil::evaluateLiteral<std::string>(*weightProperty, LogicalType::STRING(), [](std::string){return;});

    return config;
}


static uint64_t find(std::vector<std::atomic<uint64_t>>& parent, uint64_t u) 
{
    while (true) 
    {
        uint64_t p = parent[u].load(std::memory_order_acquire);
        uint64_t gp = parent[p].load(std::memory_order_acquire);
        if (p == gp)
        {
            return p;
        }
        parent[u].compare_exchange_weak(p, gp, std::memory_order_acq_rel);
        u = p;
    }
}

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

        if (parent[u].compare_exchange_strong(u, v, std::memory_order_acq_rel))
        {
            return true;
        }

        // Failed, retry
    }
}

static void filterEdges(Edges& edges, std::vector<std::atomic<uint64_t>>& parent)
{
    static constexpr uint64_t U = 0;
    static constexpr uint64_t V = 1;
    edges.erase
    (
        std::remove_if(edges.begin(), edges.end(), 
        [&](auto& e) {return find(parent, std::get<U>(e)) == find(parent, std::get<V>(e));}
        ),
        edges.end()
    );
}

static void assignCheapestEdges(Edges& edges, std::vector<std::atomic<uint64_t>>& parent, std::vector<std::optional<Edge>>& cheapest)
{
    static constexpr uint64_t U = 0;
    static constexpr uint64_t V = 1;
    static constexpr uint64_t WEIGHT = 2;

    for(const auto& e : edges)
    {
        auto u_comp = find(parent, std::get<U>(e));
        auto v_comp = find(parent, std::get<V>(e));
        // Update each component with the min edge found so far.
        auto& cu = cheapest[u_comp];
        if (cu == std::nullopt || std::get<WEIGHT>(e) < std::get<WEIGHT>(cu.value()))
        {
            cheapest[u_comp] = e;
        }
        auto& cv = cheapest[v_comp];
        if (cv == std::nullopt || std::get<WEIGHT>(e) < std::get<WEIGHT>(cv.value()))
        {
            cheapest[v_comp] = e;
        }
    }
}


static bool updateForest(Edges& finalEdges, std::vector<std::atomic<uint64_t>>& parent, std::vector<std::optional<Edge>>& cheapest)
{
    static constexpr uint64_t U = 0;
    static constexpr uint64_t V = 1;

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
                finalEdges.push_back(e.value());
                addedEdge = true;
            }
            // Need to do this since we expect cheapest container to be reset
            // for next round.
            e = std::nullopt;
        }
        return addedEdge;
}

static offset_t tableFunc(const TableFuncInput& input, TableFuncOutput&) {
    const auto clientContext = input.context->clientContext;
    auto sharedState = input.sharedState->ptrCast<GDSFuncSharedState>();
    auto graph = sharedState->graph.get();
    KU_ASSERT(graph->getNodeTableIDs().size() == 1);
    const auto tableId = graph->getNodeTableIDs()[0];
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


    Edges edges, finalEdges;
    for (auto nodeId = 0u; nodeId < numNodes; ++nodeId) {
        const nodeID_t nextNodeId = {nodeId, tableId};
        for (auto chunk : graph->scanFwd(nextNodeId, *scanState)) {
            chunk.forEach([&](auto neighbors, auto propertyVectors, auto i) {
                auto nbrId = neighbors[i].offset;
                if (nodeId < nbrId)
                {
                    auto weight = propertyVectors[0]->template getValue<uint64_t>(i);
                    edges.push_back({nodeId, nbrId, weight});
                }
            });
        }
        for (auto chunk : graph->scanBwd(nextNodeId, *scanState)) {
            chunk.forEach([&](auto neighbors, auto propertyVectors, auto i) {
                auto nbrId = neighbors[i].offset;
                if (nodeId < nbrId)
                {
                    auto weight = propertyVectors[0]->template getValue<uint64_t>(i);
                    edges.push_back({nodeId, nbrId, weight});
                }
            });
        }
    }
    // Boruvka's Algo Init Stuff.
    std::vector<std::atomic<uint64_t>> parent(numNodes);
    // Track cheapest edge coming out of a component.
    std::vector<std::optional<Edge>> cheapest(numNodes, std::nullopt);
    // Initially treat every vertex as its own component.
    for(auto i = 0u; i < parent.size(); ++i) 
    {
        parent[i].store(i, std::memory_order_relaxed);
    }

    int count = 0;
    while(++count)
    {
        // Loop over all edges finding the cheapest outgoing edge on a
        // component.
        assignCheapestEdges(edges, parent, cheapest);

        // We break if we make no progress on a round.
        bool addedEdge = updateForest(finalEdges, parent, cheapest);
        if (!addedEdge)
        {
            break;
        }
        
        // Erase edges in the same component from candidates.
        filterEdges(edges, parent);
    }
    for(offset_t i = 0; i < numNodes; ++i)
    {
        std::cout << i << std::endl;
    }

    for(auto [u, v, w] : finalEdges)
    {
        std::cout << u << " " << v << " " << w << std::endl;
    }
    return 0;
}

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    auto graphName = input->getLiteralVal<std::string>(0);
    auto graphEntry = GDSFunction::bindGraphEntry(*context, graphName);
    auto nodeOutput = GDSFunction::bindNodeOutput(*input, graphEntry.getNodeEntries());
    expression_vector columns;
    columns.push_back(nodeOutput->constCast<NodeExpression>().getInternalID());
    columns.push_back(input->binder->createVariable("U", LogicalType::INT64()));
    columns.push_back(input->binder->createVariable("V", LogicalType::INT64()));
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
