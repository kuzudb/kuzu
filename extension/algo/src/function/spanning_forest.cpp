#include "binder/binder.h"
#include "binder/expression/expression.h"
#include "binder/query/reading_clause/bound_table_function_call.h"
#include "common/assert.h"
#include "common/exception/binder.h"
#include "common/exception/runtime.h"
#include "common/in_mem_gds_utils.h"
#include "common/string_format.h"
#include "common/string_utils.h"
#include "common/types/types.h"
#include "function/algo_function.h"
#include "function/config/max_iterations_config.h"
#include "function/config/spanning_forest_config.h"
#include "function/gds/gds.h"
#include "function/gds/gds_object_manager.h"
#include "function/table/bind_input.h"
#include "main/client_context.h"
#include "planner/operator/logical_table_function_call.h"
#include "planner/planner.h"
#include "processor/execution_context.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::storage;
using namespace kuzu::graph;
using namespace kuzu::function;
using namespace kuzu::planner;

namespace kuzu {
namespace algo_extension {

// Computes a spanning forest using Kruskal's algorithm:
// - https://en.wikipedia.org/wiki/Spanning_tree#Spanning_forests
// - https://en.wikipedia.org/wiki/Kruskal%27s_algorithm

// Kruskal's algorithm iteratively adds edges to the resulting forest as long as
// they do not create a cycle. A minimum or maximum spanning forest is
// produced by processing edges in non-decreasing or non-increasing order
// of edge weights, respectively.

// Pseudocode:
// Input: G = (V, E), variant = min or max
//   G is an undirected, and potentially weighted, graph.
// Output: F = (V, E_f)
//   Where F is a forest and E_f is a subset of E.
//
// If E is weighted:
//      If `variant` is min:
//          sort(E) with non decreasing weight.
//      Else:
//          sort(E) with non increasing weight.
// For each edge e = (src, dst, weight) in E:
//      if src and dst are NOT in the same component in F (don't have the same parent):
//          Add e to E_f.
//          Mark src and dst as being in the same component (merge their parents).
// For each edge in E_f, mark its forestId as the parentId of the src node.

// The implementation here follows graaf: https://github.com/bobluppes/graaf

// Stores (srcId, dstId, relId, forestId)
using resultEdge = std::tuple<offset_t, offset_t, relID_t, offset_t>;
// Stores (srcId, dstId, relId, weight)
using weightedEdge = std::tuple<offset_t, offset_t, relID_t, double>;

/** CONFIG **/

struct SFOptionalParams final : public MaxIterationOptionalParams {
    OptionalParam<Variant> variant;
    OptionalParam<WeightProperty> weightProperty;

    explicit SFOptionalParams(const expression_vector& optionalParams);

    // For copy only
    SFOptionalParams(OptionalParam<MaxIterations> maxIterations, OptionalParam<Variant> variant,
        OptionalParam<WeightProperty> weightProperty)
        : MaxIterationOptionalParams{maxIterations}, variant{std::move(variant)},
          weightProperty{std::move(weightProperty)} {}

    void evaluateParams(main::ClientContext* context) override {
        MaxIterationOptionalParams::evaluateParams(context);
        variant.evaluateParam(context);
        weightProperty.evaluateParam(context);
    }

    std::unique_ptr<function::OptionalParams> copy() override {
        return std::make_unique<SFOptionalParams>(maxIterations, variant, weightProperty);
    }
};

SFOptionalParams::SFOptionalParams(const expression_vector& optionalParams)
    : MaxIterationOptionalParams{constructMaxIterationParam(optionalParams)} {
    for (auto& optionalParam : optionalParams) {
        auto paramName = StringUtils::getLower(optionalParam->getAlias());
        if (paramName == WeightProperty::NAME) {
            weightProperty = function::OptionalParam<WeightProperty>(optionalParam);
        } else if (paramName == Variant::NAME) {
            variant = function::OptionalParam<Variant>(optionalParam);
        } else {
            throw RuntimeException{
                stringFormat("Unknown optional argument: {}", optionalParam->getAlias())};
        }
    }
}

struct SFBindData final : public GDSBindData {
    SFBindData(expression_vector columns, graph::NativeGraphEntry graphEntry,
        expression_vector output, std::unique_ptr<SFOptionalParams> optionalParams)
        : GDSBindData{std::move(columns), std::move(graphEntry), output} {
        this->optionalParams = std::move(optionalParams);
    }

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<SFBindData>(*this);
    }
};

/** COMPUTE **/

// Runs Kruskal's algorithm.
class KruskalCompute {
public:
    KruskalCompute(storage::MemoryManager* mm, offset_t numNodes);

    // Prepares the list of edges to be processed from `graph`.
    void initEdges(Graph* graph, const table_id_t& tableId, NbrScanState* const scanState,
        const bool& weightProperty);

    // Sorts edges in ascending or descending order for computing a maximum or minimum
    // spanning forest, respectively.
    void sortEdges(const std::string& variant);

    // Implements the core of Kruskal's algorithm, as described in the class comment above.
    void run();

    // Assigns each edge an ID of the tree it belongs to. The ID internally is the final component
    // ID that the src node of each edge belongs to.
    void assignForestIds();

    const ku_vector_t<resultEdge>& getForest() const { return forest; };

    offset_t getForestSize() const { return forest.size(); };

private:
    static constexpr double DEFAULT_WEIGHT = 1;
    // Returns the component ID that `nodeId` belong to. Implemented using a disjoint-set data
    // structure (DSU).
    offset_t findComponent(const offset_t& nodeId);

    // Merges all nodes within the components indicated by `srcCompId` and `dstCompId` into a single
    // component.
    void mergeComponents(const offset_t& srcCompId, const offset_t& dstCompId);

    const offset_t numNodes;
    // Stores the edges being processed.
    ku_vector_t<weightedEdge> edges;
    // Stores the results.
    ku_vector_t<resultEdge> forest;
    // For each node, `parents[i]` points to a parent node in the same component, or to itself if
    // the node is the root of its component.
    ku_vector_t<offset_t> parents;
    // Tracks the approximate height of each component's tree.
    ku_vector_t<uint64_t> rank;
};

KruskalCompute::KruskalCompute(storage::MemoryManager* mm, offset_t numNodes)
    : numNodes{numNodes}, edges{mm}, forest{mm}, parents{mm, static_cast<size_t>(numNodes)},
      rank{mm, static_cast<size_t>(numNodes)} {
    // Mark all vertices as belonging to their own components. `std::iota` automatically
    // fills the range with sequentially increasing values starting from `0`.
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
                    // Ignore self-loops.
                    return;
                }
                auto relId = propertyVectors[0]->template getValue<relID_t>(i);
                auto weight = weightProperty ? propertyVectors[1]->template getValue<double>(i) :
                                               DEFAULT_WEIGHT;
                edges.push_back({nodeId, nbrId, relId, weight});
            });
        }
    }
}

void KruskalCompute::sortEdges(const std::string& variant) {
    const auto& compareFn = [&](const auto& e1, const auto& e2) {
        const auto& [srcId1, dstId1, relId1, weight1] = e1;
        const auto& [srcId2, dstId2, relId2, weight2] = e2;
        auto left = std::tie(weight1, srcId1, dstId1, relId1);
        auto right = std::tie(weight2, srcId2, dstId2, relId2);
        return variant == Variant::MAX_VARIANT ? left > right : left < right;
    };
    std::ranges::sort(edges, compareFn);
}

void KruskalCompute::run() {
    offset_t numEdges = 0;
    for (auto i = 0u; i < edges.size() && numEdges != numNodes - 1; ++i) {
        const auto& [srcId, dstId, relId, _] = edges[i];
        auto srcCompId = findComponent(srcId);
        auto dstCompId = findComponent(dstId);
        if (srcCompId != dstCompId) {
            ++numEdges;
            forest.push_back({srcId, dstId, relId, UINT64_MAX});
            mergeComponents(srcCompId, dstCompId);
        }
    }
}

void KruskalCompute::assignForestIds() {
    for (auto& [srcId, _dstId, _relId, forestId] : forest) {
        forestId = findComponent(srcId);
    }
}

offset_t KruskalCompute::findComponent(const offset_t& nodeId) {
    while (parents[nodeId] != parents[parents[nodeId]]) {
        parents[nodeId] = parents[parents[nodeId]];
    }
    return parents[nodeId];
}

void KruskalCompute::mergeComponents(const offset_t& srcCompId, const offset_t& dstCompId) {
    KU_ASSERT_UNCONDITIONAL(srcCompId != dstCompId);
    // Merge with the larger component, based on the rank. If ranks are equal, use IDs to break
    // ties.
    if (rank[srcCompId] == rank[dstCompId]) {
        auto newParent = std::min(srcCompId, dstCompId);
        auto newChild = std::max(srcCompId, dstCompId);
        parents[newChild] = newParent;
        rank[newParent]++;
    } else if (rank[srcCompId] < rank[dstCompId]) {
        parents[srcCompId] = dstCompId;
    } else {
        parents[dstCompId] = srcCompId;
    }
}

/** RESULTS **/

class WriteResultsSF final : public InMemResultParallelCompute {
public:
    WriteResultsSF(MemoryManager* mm, GDSFuncSharedState* sharedState,
        const ku_vector_t<resultEdge>& finalResults)
        : InMemResultParallelCompute{mm, sharedState}, finalResults{finalResults} {
        srcIdVector = createVector(LogicalType::INTERNAL_ID());
        dstIdVector = createVector(LogicalType::INTERNAL_ID());
        relIdVector = createVector(LogicalType::INTERNAL_ID());
        forestIdVector = createVector(LogicalType::UINT64());
    }

    void parallelCompute(const offset_t startOffset, const offset_t endOffset,
        const std::optional<table_id_t>& tableID) override {
        KU_ASSERT(tableID.has_value());
        for (auto i = startOffset; i < endOffset; ++i) {
            const auto& [srcId, dstId, relId, forestId] = finalResults[i];
            srcIdVector->setValue<nodeID_t>(0, nodeID_t{srcId, *tableID});
            dstIdVector->setValue<nodeID_t>(0, nodeID_t{dstId, *tableID});
            relIdVector->setValue<relID_t>(0, relId);
            forestIdVector->setValue<offset_t>(0, forestId);
            localFT->append(vectors);
        }
    }

    std::unique_ptr<InMemParallelCompute> copy() override {
        return std::make_unique<WriteResultsSF>(mm, sharedState, finalResults);
    }

private:
    const ku_vector_t<resultEdge>& finalResults;
    std::unique_ptr<ValueVector> srcIdVector;
    std::unique_ptr<ValueVector> dstIdVector;
    std::unique_ptr<ValueVector> relIdVector;
    std::unique_ptr<ValueVector> forestIdVector;
};

/** GDS Setup **/

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

    auto spanningForestBindData = input.bindData->constPtrCast<SFBindData>();
    auto& config = spanningForestBindData->optionalParams->constCast<SFOptionalParams>();
    if (!config.weightProperty.getParamVal().empty() &&
        !nbrInfo.relGroupEntry->containsProperty(config.weightProperty.getParamVal())) {
        throw RuntimeException{
            stringFormat("Cannot find property: {}", config.weightProperty.getParamVal())};
    }
    if (!config.weightProperty.getParamVal().empty() &&
        !LogicalTypeUtils::isNumerical(
            nbrInfo.relGroupEntry->getProperty(config.weightProperty.getParamVal()).getType())) {
        throw RuntimeException{stringFormat("Provided weight property is not numerical: {}",
            config.weightProperty.getParamVal())};
    }
    std::vector<std::string> relProps = {InternalKeyword::ID};
    if (!config.weightProperty.getParamVal().empty()) {
        relProps.push_back(config.weightProperty.getParamVal());
    }

    // Set randomLookup=false to enable caching during graph materialization.
    const auto scanState = graph->prepareRelScan(*nbrInfo.relGroupEntry, nbrInfo.relTableID,
        nbrInfo.dstTableID, relProps, false /*randomLookup*/);
    const auto numNodes = graph->getMaxOffset(clientContext->getTransaction(), tableId);

    KruskalCompute compute(mm, numNodes);
    compute.initEdges(graph, tableId, scanState.get(),
        !config.weightProperty.getParamVal().empty());
    compute.sortEdges(config.variant.getParamVal());
    compute.run();
    compute.assignForestIds();

    WriteResultsSF writeResults(mm, sharedState, compute.getForest());
    InMemGDSUtils::runParallelCompute(writeResults, compute.getForestSize(), input.context,
        tableId);
    sharedState->factorizedTablePool.mergeLocalTables();
    return 0;
}

static constexpr char SRC_COLUMN_NAME[] = "SRC";
static constexpr char DST_COLUMN_NAME[] = "DST";
static constexpr char REL_COLUMN_NAME[] = "REL";
static constexpr char FOREST_ID_COLUMN_NAME[] = "FOREST_ID";

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
            std::string(SpanningForest::name) + " only supports operations on one rel table.");
    }
    expression_vector columns;
    auto srcOutput =
        GDSFunction::bindNodeOutput(*input, graphEntry.getNodeEntries(), SRC_COLUMN_NAME, 0);
    auto dstOutput =
        GDSFunction::bindNodeOutput(*input, graphEntry.getNodeEntries(), DST_COLUMN_NAME, 1);
    auto relOutput = GDSFunction::bindRelOutput(*input, graphEntry.getRelEntries(),
        std::dynamic_pointer_cast<NodeExpression>(srcOutput),
        std::dynamic_pointer_cast<NodeExpression>(dstOutput), REL_COLUMN_NAME, 2);
    columns.push_back(srcOutput->constCast<NodeExpression>().getInternalID());
    columns.push_back(dstOutput->constCast<NodeExpression>().getInternalID());
    columns.push_back(relOutput->constCast<RelExpression>().getInternalID());
    columns.push_back(input->binder->createVariable(FOREST_ID_COLUMN_NAME, LogicalType::UINT64()));
    return std::make_unique<SFBindData>(std::move(columns), std::move(graphEntry),
        expression_vector{srcOutput, dstOutput, relOutput},
        std::make_unique<SFOptionalParams>(input->optionalParamsLegacy));
}

static void getLogicalPlan(Planner* planner, const BoundReadingClause& readingClause,
    expression_vector predicates, LogicalPlan& plan) {
    auto& call = readingClause.constCast<BoundTableFunctionCall>();
    auto bindData = call.getBindData()->constPtrCast<GDSBindData>();
    auto op = std::make_shared<LogicalTableFunctionCall>(call.getTableFunc(), bindData->copy());

    std::vector<std::shared_ptr<LogicalOperator>> nodeMaskPlanRoots;
    for (auto& nodeInfo : bindData->graphEntry.nodeInfos) {
        if (nodeInfo.predicate == nullptr) {
            continue;
        }
        auto& node = nodeInfo.nodeOrRel->constCast<NodeExpression>();
        planner->getCardinliatyEstimatorUnsafe().init(node);
        auto p = planner->getNodeSemiMaskPlan(SemiMaskTargetType::GDS_GRAPH_NODE, node,
            nodeInfo.predicate);
        nodeMaskPlanRoots.push_back(p.getLastOperator());
    }

    for (auto root : nodeMaskPlanRoots) {
        op->addChild(root);
    }
    op->computeFactorizedSchema();
    planner->planReadOp(std::move(op), predicates, plan);

    for (auto i = 0u; i < 2; ++i) {
        auto nodeOutput = bindData->output[i]->ptrCast<NodeExpression>();
        KU_ASSERT(nodeOutput != nullptr);
        planner->getCardinliatyEstimatorUnsafe().init(*nodeOutput);
        auto scanPlan = planner->getNodePropertyScanPlan(*nodeOutput);
        if (!scanPlan.isEmpty()) {
            expression_vector joinConditions;
            joinConditions.push_back(nodeOutput->getInternalID());
            planner->appendHashJoin(joinConditions, JoinType::INNER, plan, scanPlan, plan);
        }
    }
    auto relOutput = bindData->output[2]->ptrCast<RelExpression>();
    KU_ASSERT(relOutput != nullptr);
    auto scanPlan = LogicalPlan();
    auto boundNode = relOutput->getSrcNode();
    auto nbrNode = relOutput->getDstNode();
    const auto extendDir = ExtendDirection::FWD;
    planner->appendScanNodeTable(boundNode->getInternalID(), boundNode->getTableIDs(), {},
        scanPlan);

    auto relProperties = planner->getProperties(*relOutput);

    // If the query returns a specific relationship property (e.g., RETURN ... rel.someProperty
    // ...), getProperties() will not include the relationship's internal ID, so we need to add it
    // manually.
    //
    // If the query returns the whole relationship (e.g., RETURN ... rel ...),
    // getProperties() will include the internal ID automatically.
    // In that case, we must avoid adding it again to relProperties.
    bool foundInternalId = false;
    for (const auto& property : relProperties) {
        if (*property == *relOutput->getInternalID()) {
            foundInternalId = true;
            break;
        }
    }
    if (!foundInternalId) {
        relProperties.push_back(relOutput->getInternalID());
    }

    planner->appendExtend(boundNode, nbrNode,
        std::dynamic_pointer_cast<RelExpression>(bindData->output[2]), extendDir, relProperties,
        scanPlan);
    planner->appendProjection(relProperties, scanPlan);
    expression_vector joinConditions;
    joinConditions.push_back(relOutput->getInternalID());
    planner->appendHashJoin(joinConditions, JoinType::INNER, plan, scanPlan, plan);
}

function_set SpanningForest::getFunctionSet() {
    function_set result;
    auto func = std::make_unique<TableFunction>(name, std::vector{LogicalTypeID::ANY});
    func->bindFunc = bindFunc;
    func->tableFunc = tableFunc;
    func->initSharedStateFunc = GDSFunction::initSharedState;
    func->initLocalStateFunc = TableFunction::initEmptyLocalState;
    func->canParallelFunc = [] { return false; };
    func->getLogicalPlanFunc = getLogicalPlan;
    func->getPhysicalPlanFunc = GDSFunction::getPhysicalPlan;
    result.push_back(std::move(func));
    return result;
}

} // namespace algo_extension
} // namespace kuzu
