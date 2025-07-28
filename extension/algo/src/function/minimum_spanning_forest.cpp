#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "common/assert.h"
#include "common/exception/binder.h"
#include "common/exception/runtime.h"
#include "common/string_utils.h"
#include "common/types/types.h"
#include "function/algo_function.h"
#include "function/gds/gds.h"
#include "function/gds/gds_object_manager.h"
#include "function/gds/gds_utils.h"
#include "function/gds/gds_vertex_compute.h"
#include "function/table/bind_input.h"
#include "processor/execution_context.h"

using namespace std;
using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::storage;
using namespace kuzu::graph;
using namespace kuzu::function;

namespace kuzu {
namespace algo_extension {


class WriteResultsMSF final : public GDSResultVertexCompute {
public:
    WriteResultsMSF(MemoryManager* mm, GDSFuncSharedState* sharedState, std::vector<std::vector<std::pair<offset_t, int64_t>>>& final)
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
    std::vector<std::vector<std::pair<offset_t, int64_t>>>& finalResults;
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
    static constexpr const char* PROPERTY = "property";
    for (auto& optionalParam : optionalParams) {
        auto paramName = StringUtils::getLower(optionalParam->getAlias());
        if (paramName == PROPERTY) {
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

static offset_t find(const offset_t& u, ku_vector_t<offset_t>& parents)
{
    while(parents[u] != parents[parents[u]])
    {
        parents[u] = parents[parents[u]];
    }
    return parents[u];
}

static void mergeComponents(const offset_t& pu, const offset_t& pv, ku_vector_t<offset_t>& parents, ku_vector_t<uint64_t>& rank)
{
    KU_ASSERT_UNCONDITIONAL(pu != pv);
    if (rank[pu] == rank[pv])
    {
        auto newParent = std::min(pu, pv);
        auto newChild = std::max(pu, pv);
        parents[newChild] = newParent;
        rank[newParent]++;
    }
    else if (rank[pu] < rank[pv]) {
        parents[pu] = pv;
    }
    else {
        parents[pv] = pu;
    }
}

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

    // init dsu related structures.
    ku_vector_t<offset_t> parents(mm, numNodes);
    std::iota(parents.begin(), parents.end(), 0);
    ku_vector_t<uint64_t> rank(mm, numNodes);
    for(auto& r : rank) {
        r = 0;
    }


    // collect graph
    ku_vector_t<std::tuple<offset_t, offset_t, int64_t>> edges(mm);
    for (auto nodeId = 0u; nodeId < numNodes; ++nodeId) {
        const nodeID_t nextNodeId = {nodeId, tableId};
        for (auto chunk : graph->scanFwd(nextNodeId, *scanState)) {
            chunk.forEach([&](auto neighbors, auto propertyVectors, auto i) {
                auto nbrId = neighbors[i].offset;
                if (nbrId == nodeId) {
                    return;
                }
                auto weight = propertyVectors[0]->template getValue<int64_t>(i);
                edges.push_back({nodeId, nbrId, weight});
            });
        }
    } 

    static const auto& cmp = [&](const auto& e1, const auto& e2)
        {
            const auto& [v1, u1, w1] = e1;
            const auto& [v2, u2, w2] = e2;
            return std::tie(w1, u1, v1) > std::tie(w2, u2, v2);
        };


    std::make_heap(edges.begin(), edges.end(), cmp);
    // Cannot be init as done below. MmAllocator' has been explicitly marked deleted here
    // ku_vector_t<ku_vector_t<std::pair<offset_t, int64_t>>> forest(mm);
    // for(int i = 0; i < numNodes; ++i)
    //{
    //  forest.emplace_back({mm});
    //}
    std::vector<std::vector<std::pair<offset_t, int64_t>>> forest(numNodes);
    offset_t numEdges = 0;
    while(!edges.empty() && numEdges != numNodes - 1)
    {
        std::pop_heap(edges.begin(), edges.end(), cmp);
        const auto [u, v, w] = edges.back();
        edges.pop_back();
        auto pu = find(u, parents);
        auto pv = find(v, parents);
        if (pu != pv)
        {
            ++numEdges;
            forest[u].push_back({v, w});
            forest[v].push_back({u, w});
            mergeComponents(pu, pv, parents, rank);
        }
    }

    const auto vertexCompute = make_unique<WriteResultsMSF>(mm, sharedState, forest);
    GDSUtils::runVertexCompute(input.context, GDSDensityState::DENSE, graph, *vertexCompute);

    sharedState->factorizedTablePool.mergeLocalTables();

    return 0;
}

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    auto graphName = input->getLiteralVal<std::string>(0);
    auto graphEntry = GDSFunction::bindGraphEntry(*context, graphName);
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
