#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "common/assert.h"
#include "common/exception/binder.h"
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

// resultEdge: (src_id, dst_id, rel_id, forest_id)
// weightedEdge: (src_id, dst_id, rel_id, weight)
using resultEdge = std::tuple<offset_t, offset_t, relID_t, offset_t>;
using weightedEdge = std::tuple<offset_t, offset_t, relID_t, double>;

class WriteResultsSF final : public GDSResultVertexCompute {
public:
    WriteResultsSF(MemoryManager* mm, GDSFuncSharedState* sharedState,
        const ku_vector_t<resultEdge>& finalResults)
        : GDSResultVertexCompute{mm, sharedState}, finalResults{finalResults} {
        srcIDVector = createVector(LogicalType::INTERNAL_ID());
        dstIDVector = createVector(LogicalType::INTERNAL_ID());
        relIDVector = createVector(LogicalType::INTERNAL_ID());
        forestIDVector = createVector(LogicalType::UINT64());
    }

    void beginOnTableInternal(table_id_t /*tableID*/) override {}

    void vertexCompute(const offset_t startOffset, const offset_t endOffset,
        const table_id_t tableID) override {
        for (auto i = startOffset; i < endOffset; ++i) {
            const auto& [srcId, dstId, relId, forestId] = finalResults[i];
            srcIDVector->setValue<nodeID_t>(0, nodeID_t{srcId, tableID});
            dstIDVector->setValue<nodeID_t>(0, nodeID_t{dstId, tableID});
            relIDVector->setValue<relID_t>(0, relId);
            forestIDVector->setValue<offset_t>(0, forestId);
            localFT->append(vectors);
        }
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<WriteResultsSF>(mm, sharedState, finalResults);
    }

private:
    const ku_vector_t<resultEdge>& finalResults;
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
    if (weightProperty != nullptr) {
        config->weightProperty =
            ExpressionUtil::evaluateLiteral<std::string>(*weightProperty, LogicalType::STRING());
    }
    if (maxForest != nullptr) {
        config->maxForest = ExpressionUtil::evaluateLiteral<bool>(*maxForest, LogicalType::BOOL());
    }
    return config;
}

class KruskalCompute {
public:
    KruskalCompute(storage::MemoryManager* mm, offset_t numNodes);

    void initEdges(Graph* graph, const table_id_t& tableId, NbrScanState* const scanState,
        const bool& weightProperty);

    void sortEdges(const bool& maxForest);

    void run();

    // Assigns each edge an ID of the tree it belongs to. The ID internally is the final component
    // ID that the src node of each edge belongs to in the disjoint set data structure used in the
    // computation.
    void assignForestIds();

    const ku_vector_t<resultEdge>& getForest() const { return forest; };

    offset_t getForestSize() const { return forest.size(); };

private:
    static constexpr double DEFAULTWEIGHT = 1;
    // Returns the component ID that `nodeID` belong to. Implemented using a Disjoint-set data
    // structure (DSU).
    offset_t findComponent(const offset_t& nodeId);

    // Merges all nodes within the components indicated by `srcCompID` and `dstCompID` into a single
    // component.
    void mergeComponents(const offset_t& pSrcId, const offset_t& pDstId);

    const offset_t numNodes;
    ku_vector_t<weightedEdge> edges;
    ku_vector_t<offset_t> parents;
    ku_vector_t<uint64_t> rank;
    ku_vector_t<resultEdge> forest;
};

KruskalCompute::KruskalCompute(storage::MemoryManager* mm, offset_t numNodes)
    : numNodes{numNodes}, edges{mm}, parents{mm, static_cast<size_t>(numNodes)},
      rank{mm, static_cast<size_t>(numNodes)}, forest{mm} {
    // Mark all vertices as belonging to their own components.
    std::iota(parents.begin(), parents.end(), 0);
}

void KruskalCompute::initEdges(Graph* graph, const table_id_t& tableId,
    NbrScanState* const scanState, const bool& weightProperty) {
    for (auto nodeId = 0u; nodeId < numNodes; ++nodeId) {
        const nodeID_t nextNodeId = {nodeId, tableId};
        for (auto chunk : graph->scanFwd(nextNodeId, *scanState)) {
            chunk.forEach([&](auto neighbors, auto propertyVectors, auto i) {
                auto nbrId = neighbors[i].offset;
                if (nbrId == nodeId) {
                    // Ignore self loops.
                    return;
                }
                auto relId = propertyVectors[0]->template getValue<relID_t>(i);
                auto weight = weightProperty ? propertyVectors[1]->template getValue<double>(i) :
                                               DEFAULTWEIGHT;
                edges.push_back({nodeId, nbrId, relId, weight});
            });
        }
    }
}

void KruskalCompute::sortEdges(const bool& maxForest) {
    const auto& compareFn = [&](const auto& e1, const auto& e2) {
        const auto& [srcId1, dstId1, relId1, weight1] = e1;
        const auto& [srcId2, dstId2, relId2, weight2] = e2;
        return maxForest ? std::tie(weight1, srcId1, dstId1, relId1) > std::tie(weight2, srcId2, dstId2, relId2) :
                           std::tie(weight1, srcId1, dstId1, relId1) < std::tie(weight2, srcId2, dstId2, relId2);
    };
    std::ranges::sort(edges, compareFn);
}

void KruskalCompute::run() {
    offset_t numEdges = 0;
    for (auto i = 0u; i < edges.size() && numEdges != numNodes - 1; ++i) {
        const auto& [srcId, dstId, relId, _] = edges[i];
        auto pSrcId = findComponent(srcId);
        auto pDstId = findComponent(dstId);
        if (pSrcId != pDstId) {
            ++numEdges;
            forest.push_back({srcId, dstId, relId, UINT64_MAX});
            mergeComponents(pSrcId, pDstId);
        }
    }
}

void KruskalCompute::assignForestIds() {
    for (auto& [srcId, dstId, relId, forestId] : forest) {
        forestId = findComponent(srcId);
    }
}

offset_t KruskalCompute::findComponent(const offset_t& nodeId) {
    while (parents[nodeId] != parents[parents[nodeId]]) {
        parents[nodeId] = parents[parents[nodeId]];
    }
    return parents[nodeId];
}

void KruskalCompute::mergeComponents(const offset_t& pSrcId, const offset_t& pDstId) {
    KU_ASSERT_UNCONDITIONAL(pSrcId != pDstId);
    if (rank[pSrcId] == rank[pDstId]) {
        auto newParent = std::min(pSrcId, pDstId);
        auto newChild = std::max(pSrcId, pDstId);
        parents[newChild] = newParent;
        rank[newParent]++;
    } else if (rank[pSrcId] < rank[pDstId]) {
        parents[pSrcId] = pDstId;
    } else {
        parents[pDstId] = pSrcId;
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
    std::vector<std::string> relProps = {InternalKeyword::ID};
    if (!config.weightProperty.empty()) {
        relProps.push_back(config.weightProperty);
    }
    const auto scanState = graph->prepareRelScan(*nbrInfo.relGroupEntry, nbrInfo.relTableID,
        nbrInfo.dstTableID, relProps, false /*randomLookup*/);
    const auto numNodes = graph->getMaxOffset(clientContext->getTransaction(), tableId);

    KruskalCompute compute(mm, numNodes);
    compute.initEdges(graph, tableId, scanState.get(), !config.weightProperty.empty());
    compute.sortEdges(config.maxForest);
    compute.run();
    compute.assignForestIds();

    const auto writeResultsVC = make_unique<WriteResultsSF>(mm, sharedState, compute.getForest());
    GDSUtils::runVertexCompute(input.context, GDSDensityState::DENSE, graph, *writeResultsVC,
        compute.getForestSize());
    sharedState->factorizedTablePool.mergeLocalTables();
    return 0;
}

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    auto graphName = input->getLiteralVal<std::string>(0);
    auto graphEntry = GDSFunction::bindGraphEntry(*context, graphName);
    if (graphEntry.nodeInfos.size() != 1) {
        throw BinderException(
            std::string(SpanningForest::name) + " only supports operations on one node table.");
    }
    if (graphEntry.relInfos.size() != 1) {
        throw BinderException(
            std::string(SpanningForest::name) + " only supports operations on one edge table.");
    }
    auto optionalParams = make_unique<MSFOptionalParams>(input->optionalParamsLegacy);
    auto config = optionalParams->getConfig()->constCast<MSFConfig>();
    if (!config.weightProperty.empty() &&
        !graphEntry.relInfos[0].entry->containsProperty(config.weightProperty)) {
        throw BinderException("Cannot find property: " + config.weightProperty);
    }
    if (!config.weightProperty.empty() &&
        !LogicalTypeUtils::isNumerical(
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

function_set SpanningForest::getFunctionSet() {
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
