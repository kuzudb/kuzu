#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "common/exception/binder.h"
#include "common/string_utils.h"
#include "degrees.h"
#include "function/gds/config/page_rank_config.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/gds_utils.h"
#include "gds_vertex_compute.h"
#include "processor/execution_context.h"

using namespace kuzu::processor;
using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::storage;
using namespace kuzu::graph;

namespace kuzu {
namespace function {

struct PageRankOptionalParams {
    std::shared_ptr<binder::Expression> dampingFactor;
    std::shared_ptr<binder::Expression> maxIteration;
    std::shared_ptr<binder::Expression> tolerance;

    explicit PageRankOptionalParams(const binder::expression_vector& optionalParams);

    PageRankConfig getConfig() const;
};

PageRankOptionalParams::PageRankOptionalParams(const binder::expression_vector& optionalParams) {
    for (auto& optionalParam : optionalParams) {
        auto paramName = StringUtils::getLower(optionalParam->getAlias());
        if (paramName == DampingFactor::NAME) {
            dampingFactor = optionalParam;
        } else if (paramName == MaxIterations::NAME) {
            maxIteration = optionalParam;
        } else if (paramName == Tolerance::NAME) {
            tolerance = optionalParam;
        } else {
            throw common::BinderException{
                "Unknown optional parameter: " + optionalParam->getAlias()};
        }
    }
}

PageRankConfig PageRankOptionalParams::getConfig() const {
    PageRankConfig config;
    if (dampingFactor != nullptr) {
        config.dampingFactor = ExpressionUtil::evaluateLiteral<double>(*dampingFactor,
            LogicalType::DOUBLE(), DampingFactor::validate);
    }
    if (maxIteration != nullptr) {
        config.maxIterations = ExpressionUtil::evaluateLiteral<int64_t>(*maxIteration,
            LogicalType::INT64(), MaxIterations::validate);
    }
    if (tolerance != nullptr) {
        config.tolerance =
            ExpressionUtil::evaluateLiteral<double>(*tolerance, LogicalType::DOUBLE());
    }
    return config;
}

struct PageRankBindData final : public GDSBindData {
    PageRankOptionalParams optionalParams;

    PageRankBindData(binder::expression_vector columns, graph::GraphEntry graphEntry,
        std::shared_ptr<binder::Expression> nodeOutput, PageRankOptionalParams optionalParams)
        : GDSBindData{std::move(columns), std::move(graphEntry), std::move(nodeOutput)},
          optionalParams{std::move(optionalParams)} {}
    PageRankBindData(const PageRankBindData& other)
        : GDSBindData{other}, optionalParams{other.optionalParams} {}

    PageRankConfig getConfig() const { return optionalParams.getConfig(); }

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<PageRankBindData>(*this);
    }
};

static void addCAS(std::atomic<double>& origin, double valToAdd) {
    auto expected = origin.load(std::memory_order_relaxed);
    auto desired = expected + valToAdd;
    while (!origin.compare_exchange_strong(expected, desired)) {
        desired = expected + valToAdd;
    }
}

// Represents PageRank value for all nodes
class PValues {
public:
    PValues(table_id_map_t<offset_t> maxOffsetMap, storage::MemoryManager* mm, double val) {
        for (const auto& [tableID, maxOffset] : maxOffsetMap) {
            valueMap.allocate(tableID, maxOffset, mm);
            pinTable(tableID);
            for (auto i = 0u; i < maxOffset; ++i) {
                values[i].store(val, std::memory_order_relaxed);
            }
        }
    }

    void pinTable(table_id_t tableID) { values = valueMap.getData(tableID); }

    double getValue(offset_t offset) { return values[offset].load(std::memory_order_relaxed); }

    void addValueCAS(offset_t offset, double val) { addCAS(values[offset], val); }

    void setValue(offset_t offset, double val) {
        values[offset].store(val, std::memory_order_relaxed);
    }

private:
    std::atomic<double>* values = nullptr;
    ObjectArraysMap<std::atomic<double>> valueMap;
};

class PageRankAuxiliaryState : public GDSAuxiliaryState {
public:
    PageRankAuxiliaryState(Degrees& degrees, PValues& pCurrent, PValues& pNext)
        : degrees{degrees}, pCurrent{pCurrent}, pNext{pNext} {}

    void beginFrontierCompute(table_id_t fromTableID, table_id_t toTableID) override {
        degrees.pinTable(toTableID);
        pCurrent.pinTable(toTableID);
        pNext.pinTable(fromTableID);
    }

private:
    Degrees& degrees;
    PValues& pCurrent;
    PValues& pNext;
};

// Sum the weight (current rank / degree) for each incoming edge.
class PNextUpdateEdgeCompute : public EdgeCompute {
public:
    PNextUpdateEdgeCompute(Degrees& degrees, PValues& pCurrent, PValues& pNext)
        : degrees{degrees}, pCurrent{pCurrent}, pNext{pNext} {}

    std::vector<nodeID_t> edgeCompute(nodeID_t boundNodeID, graph::NbrScanState::Chunk& chunk,
        bool) override {
        if (chunk.size() > 0) {
            double valToAdd = 0;
            chunk.forEach([&](auto nbrNodeID, auto) {
                valToAdd +=
                    pCurrent.getValue(nbrNodeID.offset) / degrees.getValue(nbrNodeID.offset);
            });
            pNext.addValueCAS(boundNodeID.offset, valToAdd);
        }
        return {};
    }

    std::unique_ptr<EdgeCompute> copy() override {
        return std::make_unique<PNextUpdateEdgeCompute>(degrees, pCurrent, pNext);
    }

private:
    Degrees& degrees;
    PValues& pCurrent;
    PValues& pNext;
};

// Evaluate rank = above result * dampingFactor + {(1 - dampingFactor) / |V|} (constant)
class PNextUpdateVertexCompute : public GDSVertexCompute {
public:
    PNextUpdateVertexCompute(double dampingFactor, double constant, PValues& pNext,
        NodeOffsetMaskMap* nodeMask)
        : GDSVertexCompute{nodeMask}, dampingFactor{dampingFactor}, constant{constant},
          pNext{pNext} {}

    void beginOnTableInternal(common::table_id_t tableID) override { pNext.pinTable(tableID); }

    void vertexCompute(offset_t startOffset, offset_t endOffset, table_id_t) override {
        for (auto i = startOffset; i < endOffset; ++i) {
            if (skip(i)) {
                continue;
            }
            pNext.setValue(i, pNext.getValue(i) * dampingFactor + constant);
        }
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<PNextUpdateVertexCompute>(dampingFactor, constant, pNext, nodeMask);
    }

private:
    double dampingFactor;
    double constant;
    PValues& pNext;
};

class PDiffVertexCompute : public GDSVertexCompute {
public:
    PDiffVertexCompute(std::atomic<double>& diff, PValues& pCurrent, PValues& pNext,
        NodeOffsetMaskMap* nodeMask)
        : GDSVertexCompute{nodeMask}, diff{diff}, pCurrent{pCurrent}, pNext{pNext} {}

    void beginOnTableInternal(table_id_t tableID) override {
        pCurrent.pinTable(tableID);
        pNext.pinTable(tableID);
    }

    void vertexCompute(offset_t startOffset, offset_t endOffset, table_id_t) override {
        for (auto i = startOffset; i < endOffset; ++i) {
            if (skip(i)) {
                continue;
            }
            auto next = pNext.getValue(i);
            auto current = pCurrent.getValue(i);
            if (next > current) {
                addCAS(diff, next - current);
            } else {
                addCAS(diff, current - next);
            }
            pCurrent.setValue(i, 0);
        }
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<PDiffVertexCompute>(diff, pCurrent, pNext, nodeMask);
    }

private:
    std::atomic<double>& diff;
    PValues& pCurrent;
    PValues& pNext;
};

class PageRankResultVertexCompute : public GDSResultVertexCompute {
public:
    PageRankResultVertexCompute(storage::MemoryManager* mm, GDSFuncSharedState* sharedState,
        PValues& pNext)
        : GDSResultVertexCompute{mm, sharedState}, pNext{pNext} {
        nodeIDVector = createVector(LogicalType::INTERNAL_ID());
        rankVector = createVector(LogicalType::DOUBLE());
    }

    void beginOnTableInternal(table_id_t tableID) override { pNext.pinTable(tableID); }

    void vertexCompute(offset_t startOffset, offset_t endOffset, table_id_t tableID) override {
        for (auto i = startOffset; i < endOffset; ++i) {
            if (skip(i)) {
                continue;
            }
            auto nodeID = nodeID_t{i, tableID};
            nodeIDVector->setValue<nodeID_t>(0, nodeID);
            rankVector->setValue<double>(0, pNext.getValue(i));
            localFT->append(vectors);
        }
    }

    std::unique_ptr<VertexCompute> copy() override {
        return std::make_unique<PageRankResultVertexCompute>(mm, sharedState, pNext);
    }

private:
    PValues& pNext;
    std::unique_ptr<ValueVector> nodeIDVector;
    std::unique_ptr<ValueVector> rankVector;
};

static common::offset_t tableFunc(const TableFuncInput& input, TableFuncOutput&) {
    auto clientContext = input.context->clientContext;
    auto transaction = clientContext->getTransaction();
    auto sharedState = input.sharedState->ptrCast<GDSFuncSharedState>();
    auto graph = sharedState->graph.get();
    auto maxOffsetMap = graph->getMaxOffsetMap(transaction);
    auto numNodes = graph->getNumNodes(transaction);
    auto p1 = PValues(maxOffsetMap, clientContext->getMemoryManager(), (double)1 / numNodes);
    auto p2 = PValues(maxOffsetMap, clientContext->getMemoryManager(), 0);
    PValues* pCurrent = &p1;
    PValues* pNext = &p2;
    auto pageRankBindData = input.bindData->constPtrCast<PageRankBindData>();
    auto config = pageRankBindData->getConfig();
    auto currentIter = 1u;
    auto currentFrontier =
        PathLengths::getVisitedFrontier(input.context, graph, sharedState->getGraphNodeMaskMap());
    auto nextFrontier =
        PathLengths::getVisitedFrontier(input.context, graph, sharedState->getGraphNodeMaskMap());
    auto degrees = Degrees(maxOffsetMap, clientContext->getMemoryManager());
    DegreesUtils::computeDegree(input.context, graph, sharedState->getGraphNodeMaskMap(), &degrees,
        ExtendDirection::FWD);
    auto frontierPair =
        std::make_unique<DoublePathLengthsFrontierPair>(currentFrontier, nextFrontier);
    auto computeState = GDSComputeState(std::move(frontierPair), nullptr, nullptr, nullptr);
    auto pNextUpdateConstant = (1 - config.dampingFactor) * ((double)1 / numNodes);
    while (currentIter < config.maxIterations) {
        computeState.frontierPair->initState();
        computeState.frontierPair->initGDS();
        computeState.edgeCompute =
            std::make_unique<PNextUpdateEdgeCompute>(degrees, *pCurrent, *pNext);
        computeState.auxiliaryState =
            std::make_unique<PageRankAuxiliaryState>(degrees, *pCurrent, *pNext);
        GDSUtils::runFrontiersUntilConvergence(input.context, computeState, graph,
            ExtendDirection::BWD, 1);
        auto pNextUpdateVC = PNextUpdateVertexCompute(config.dampingFactor, pNextUpdateConstant,
            *pNext, sharedState->getGraphNodeMaskMap());
        GDSUtils::runVertexCompute(input.context, graph, pNextUpdateVC);
        std::atomic<double> diff;
        diff.store(0);
        auto pDiffVC =
            PDiffVertexCompute(diff, *pCurrent, *pNext, sharedState->getGraphNodeMaskMap());
        GDSUtils::runVertexCompute(input.context, graph, pDiffVC);
        std::swap(pCurrent, pNext);
        if (diff.load() < config.tolerance) { // Converged.
            break;
        }
        currentIter++;
    }
    auto outputVC = std::make_unique<PageRankResultVertexCompute>(clientContext->getMemoryManager(),
        sharedState, *pCurrent);
    GDSUtils::runVertexCompute(input.context, graph, *outputVC);
    sharedState->factorizedTablePool.mergeLocalTables();
    return 0;
}

static constexpr char RANK_COLUMN_NAME[] = "rank";

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* context,
    const TableFuncBindInput* input) {
    auto graphName = input->getLiteralVal<std::string>(0);
    auto graphEntry = GDSFunction::bindGraphEntry(*context, graphName);
    auto nodeOutput = GDSFunction::bindNodeOutput(*input, graphEntry.getNodeEntries());
    expression_vector columns;
    columns.push_back(nodeOutput->constCast<NodeExpression>().getInternalID());
    columns.push_back(input->binder->createVariable(RANK_COLUMN_NAME, LogicalType::DOUBLE()));
    return std::make_unique<PageRankBindData>(std::move(columns), std::move(graphEntry), nodeOutput,
        PageRankOptionalParams(input->optionalParamsLegacy));
}

function_set PageRankFunction::getFunctionSet() {
    function_set result;
    auto func = std::make_unique<TableFunction>(PageRankFunction::name,
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
