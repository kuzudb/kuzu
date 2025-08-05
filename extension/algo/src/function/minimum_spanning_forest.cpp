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
    WriteResultsMSF(MemoryManager* mm, GDSFuncSharedState* sharedState,
        ku_vector_t<std::tuple<offset_t, offset_t, relID_t, offset_t>>& final)
        : GDSResultVertexCompute{mm, sharedState}, finalResults{final} {
        srcIDVector = createVector(LogicalType::INTERNAL_ID());
        dstIDVector = createVector(LogicalType::INTERNAL_ID());
        relIDVector = createVector(LogicalType::INTERNAL_ID());
        forestIDVector = createVector(LogicalType::UINT64());
    }

    void beginOnTableInternal(table_id_t /*tableID*/) override {}

    void vertexCompute(const offset_t startOffset, const offset_t endOffset,
        const table_id_t tableID) override {
        for (auto i = startOffset; i < endOffset; ++i) {
            const auto& [u, v, r, f] = finalResults[i];
            srcIDVector->setValue<nodeID_t>(0, nodeID_t{u, tableID});
            dstIDVector->setValue<nodeID_t>(0, nodeID_t{v, tableID});
            relIDVector->setValue<relID_t>(0, r);
            forestIDVector->setValue<offset_t>(0, f);
            localFT->append(vectors);
        }
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<WriteResultsMSF>(mm, sharedState, finalResults);
    }

private:
    ku_vector_t<std::tuple<offset_t, offset_t, relID_t, offset_t>>& finalResults;
    std::unique_ptr<ValueVector> srcIDVector;
    std::unique_ptr<ValueVector> dstIDVector;
    std::unique_ptr<ValueVector> relIDVector;
    std::unique_ptr<ValueVector> forestIDVector;
};

struct MSFConfig final : public GDSConfig {
    std::string weightProperty;
    bool maxForest = false;
    MSFConfig() = default;
};

struct MSFOptionalParams final : public GDSOptionalParams {
    std::shared_ptr<Expression> weightProperty;
    std::shared_ptr<Expression> maxForest;

    explicit MSFOptionalParams(const expression_vector& optionalParams);

    std::unique_ptr<GDSConfig> getConfig() const override;

    std::unique_ptr<GDSOptionalParams> copy() const override {
        return std::make_unique<MSFOptionalParams>(*this);
    }
};

MSFOptionalParams::MSFOptionalParams(const expression_vector& optionalParams) {
    static constexpr const char* WEIGHTPROPERTY = "weight_property";
    static constexpr const char* MAXFOREST = "max_forest";
    for (auto& optionalParam : optionalParams) {
        auto paramName = StringUtils::getLower(optionalParam->getAlias());
        if (paramName == WEIGHTPROPERTY) {
            weightProperty = optionalParam;
        } else if (paramName == MAXFOREST) {
            maxForest = optionalParam;
        } else {
            throw BinderException{"Unknown optional parameter: " + optionalParam->getAlias()};
        }
    }
}

std::unique_ptr<GDSConfig> MSFOptionalParams::getConfig() const {
    auto config = std::make_unique<MSFConfig>();
    if (weightProperty == nullptr) {
        throw RuntimeException("No parameter specifying the weight\n");
    }
    config->weightProperty =
        ExpressionUtil::evaluateLiteral<std::string>(*weightProperty, LogicalType::STRING());
    if (maxForest != nullptr) {
        config->maxForest = ExpressionUtil::evaluateLiteral<bool>(*maxForest, LogicalType::BOOL());
    }
    return config;
}

// find and mergeComponents implement DSU to track vertices and their associated
// components such that we do not add a cycle to our forest.
// https://en.wikipedia.org/wiki/Disjoint-set_data_structure
static offset_t find(const offset_t& u, ku_vector_t<offset_t>& parents) {
    while (parents[u] != parents[parents[u]]) {
        parents[u] = parents[parents[u]];
    }
    return parents[u];
}

static void mergeComponents(const offset_t& pu, const offset_t& pv, ku_vector_t<offset_t>& parents,
    ku_vector_t<uint64_t>& rank) {
    KU_ASSERT_UNCONDITIONAL(pu != pv);
    if (rank[pu] == rank[pv]) {
        auto newParent = std::min(pu, pv);
        auto newChild = std::max(pu, pv);
        parents[newChild] = newParent;
        rank[newParent]++;
    } else if (rank[pu] < rank[pv]) {
        parents[pu] = pv;
    } else {
        parents[pv] = pu;
    }
}

struct KruskalState {
    ku_vector_t<std::tuple<offset_t, offset_t, relID_t, double>> edges;
    ku_vector_t<offset_t> parents;
    ku_vector_t<uint64_t> rank;
    ku_vector_t<std::tuple<offset_t, offset_t, relID_t, offset_t>> forest;
    KruskalState(storage::MemoryManager* mm, offset_t numNodes)
        : edges{mm}, parents{mm, static_cast<size_t>(numNodes)},
          rank{mm, static_cast<size_t>(numNodes)}, forest{mm} {
        // parents[i] = i;
        std::iota(parents.begin(), parents.end(), 0);
    }
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
    const auto scanState = graph->prepareRelScan(*nbrInfo.relGroupEntry, nbrInfo.relTableID,
        nbrInfo.dstTableID, {InternalKeyword::ID, config.weightProperty}, false /*randomLookup*/);
    const auto numNodes = graph->getMaxOffset(clientContext->getTransaction(), tableId);

    KruskalState state(mm, numNodes);

    for (auto nodeId = 0u; nodeId < numNodes; ++nodeId) {
        const nodeID_t nextNodeId = {nodeId, tableId};
        for (auto chunk : graph->scanFwd(nextNodeId, *scanState)) {
            chunk.forEach([&](auto neighbors, auto propertyVectors, auto i) {
                auto nbrId = neighbors[i].offset;
                if (nbrId == nodeId) {
                    return;
                }
                auto relId = propertyVectors[0]->template getValue<relID_t>(i);
                auto weight = propertyVectors[1]->template getValue<double>(i);
                state.edges.push_back({nodeId, nbrId, relId, weight});
            });
        }
    }

    const auto& cmp = [&](const auto& e1, const auto& e2) {
        const auto& [u1, v1, r1, w1] = e1;
        const auto& [u2, v2, r2, w2] = e2;
        return config.maxForest ? std::tie(w1, u1, v1, r1) > std::tie(w2, u2, v2, r2) :
                                  std::tie(w1, u1, v1, r1) < std::tie(w2, u2, v2, r2);
    };

    std::sort(state.edges.begin(), state.edges.end(), cmp);
    offset_t numEdges = 0;
    for (auto i = 0u; i < state.edges.size() && numEdges != numNodes - 1; ++i) {
        const auto& [u, v, r, _] = state.edges[i];
        auto pu = find(u, state.parents);
        auto pv = find(v, state.parents);
        if (pu != pv) {
            ++numEdges;
            state.forest.push_back({u, v, r, UINT64_MAX});
            mergeComponents(pu, pv, state.parents, state.rank);
        }
    }
    // Assign each rel a forest id. In this case we use the node id
    // of the parent of the component u is associated with.
    for (auto& [u, v, r, f] : state.forest) {
        f = find(u, state.parents);
    }
    const auto vertexCompute = make_unique<WriteResultsMSF>(mm, sharedState, state.forest);
    GDSUtils::runVertexCompute(input.context, GDSDensityState::DENSE, graph, *vertexCompute,
        state.forest.size());
    sharedState->factorizedTablePool.mergeLocalTables();
    return 0;
}

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    auto graphName = input->getLiteralVal<std::string>(0);
    auto graphEntry = GDSFunction::bindGraphEntry(*context, graphName);
    if (graphEntry.nodeInfos.size() != 1) {
        throw BinderException(std::string(MinimumSpanningForest::name) +
                              " only supports operations on one node table.");
    }
    if (graphEntry.relInfos.size() != 1) {
        throw BinderException(std::string(MinimumSpanningForest::name) +
                              " only supports operations on one edge table.");
    }
    auto optionalParams = make_unique<MSFOptionalParams>(input->optionalParamsLegacy);
    auto config = optionalParams->getConfig()->constCast<MSFConfig>();
    if (!graphEntry.relInfos[0].entry->containsProperty(config.weightProperty)) {
        throw BinderException("Cannot find property: " + config.weightProperty);
    }
    if (!LogicalTypeUtils::isNumerical(
            graphEntry.relInfos[0].entry->getProperty(config.weightProperty).getType())) {
        throw BinderException(
            "Provided weight property is not numerical: " + config.weightProperty);
    }
    auto nodeOutput = GDSFunction::bindNodeOutput(*input, graphEntry.getNodeEntries(), "src");
    expression_vector columns;
    columns.push_back(nodeOutput->constCast<NodeExpression>().getInternalID());
    columns.push_back(input->binder->createVariable("DST", LogicalType::INTERNAL_ID()));
    columns.push_back(input->binder->createVariable("REL", LogicalType::INTERNAL_ID()));
    columns.push_back(input->binder->createVariable("FOREST_ID", LogicalType::UINT64()));
    return std::make_unique<GDSBindData>(std::move(columns), std::move(graphEntry), nodeOutput,
        std::move(optionalParams));
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
