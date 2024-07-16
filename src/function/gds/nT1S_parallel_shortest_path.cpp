#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
#include "common/types/types.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/ife_morsel.h"
#include "function/gds/parallel_shortest_path_commons.h"
#include "function/gds/parallel_utils.h"
#include "function/gds_function.h"
#include "graph/in_mem_graph.h"

using namespace kuzu::common;
using namespace kuzu::binder;

namespace kuzu {
namespace function {

class nT1SParallelShortestPath : public GDSAlgorithm {
public:
    nT1SParallelShortestPath() = default;
    nT1SParallelShortestPath(const nT1SParallelShortestPath& other) : GDSAlgorithm{other} {}

    /*
     * Inputs are
     *
     * graph::ANY
     * srcNode::NODE
     * upperBound::INT64
     */
    std::vector<common::LogicalTypeID> getParameterTypeIDs() const override {
        return {LogicalTypeID::ANY, LogicalTypeID::NODE, LogicalTypeID::INT64};
    }

    /*
     * Outputs are
     *
     * srcNode._id::INTERNAL_ID
     * dst::INTERNAL_ID
     * length::INT64
     */
    binder::expression_vector getResultColumns(binder::Binder* binder) const override {
        expression_vector columns;
        columns.push_back(bindData->nodeInput->constCast<NodeExpression>().getInternalID());
        columns.push_back(binder->createVariable("dst", LogicalType::INTERNAL_ID()));
        columns.push_back(binder->createVariable("length", LogicalType::INT64()));
        return columns;
    }

    void bind(const binder::expression_vector& params) override {
        KU_ASSERT(params.size() == 3);
        auto inputNode = params[1];
        ExpressionUtil::validateExpressionType(*params[2], ExpressionType::LITERAL);
        ExpressionUtil::validateDataType(*params[2], LogicalType::INT64());
        auto upperBound = params[2]->constCast<LiteralExpression>().getValue().getValue<int64_t>();
        bindData = std::make_unique<ParallelShortestPathBindData>(inputNode, upperBound);
    }

    void initLocalState(main::ClientContext* context) override {
        localState = std::make_unique<ParallelShortestPathLocalState>();
        localState->init(context);
    }

    static std::pair<uint64_t, uint64_t> visitNbrs(IFEMorsel* ifeMorsel,
        ValueVector& dstNodeIDVector) {
        uint64_t numDstVisitedLocal = 0u, numNonDstVisitedLocal = 0u;
        auto size = dstNodeIDVector.state->getSelVector().getSelSize();
        auto nbrNodes = (common::nodeID_t*)dstNodeIDVector.getData();
        common::nodeID_t dstNodeID;
        uint8_t state;
        for (auto j = 0u; j < size; j++) {
            dstNodeID = nbrNodes[j];
            state = ifeMorsel->visitedNodes[dstNodeID.offset];
            if (state == NOT_VISITED_DST) {
                if (state == __atomic_exchange_n(&ifeMorsel->visitedNodes[dstNodeID.offset],
                                 VISITED_DST, __ATOMIC_ACQ_REL)) {
                    numDstVisitedLocal++;
                    __atomic_store_n(&ifeMorsel->pathLength[dstNodeID.offset],
                        ifeMorsel->currentLevel + 1, __ATOMIC_RELAXED);
                    __atomic_store_n(&ifeMorsel->nextFrontier[dstNodeID.offset], 1u,
                        __ATOMIC_RELAXED);
                }
            } else if (state == NOT_VISITED) {
                if (state == __atomic_exchange_n(&ifeMorsel->visitedNodes[dstNodeID.offset],
                                 VISITED, __ATOMIC_ACQ_REL)) {
                    numNonDstVisitedLocal++;
                    __atomic_store_n(&ifeMorsel->nextFrontier[dstNodeID.offset], 1u,
                        __ATOMIC_RELAXED);
                }
            }
        }
        return {numDstVisitedLocal, numNonDstVisitedLocal};
    }

    static std::pair<uint64_t, uint64_t> visitNbrs(common::offset_t frontierOffset,
        IFEMorsel* ifeMorsel, graph::Graph* graph) {
        uint64_t numDstVisitedLocal = 0u, numNonDstVisitedLocal = 0u;
        auto inMemGraph = ku_dynamic_cast<graph::Graph*, graph::InMemGraph*>(graph);
        auto& csr = inMemGraph->getInMemCSR();
        auto csrEntry = csr[frontierOffset >> RIGHT_SHIFT];
        if (!csrEntry) {
            return {0, 0};
        }
        auto posInCSR = frontierOffset & OFFSET_DIV;
        for (auto nbrIdx = csrEntry->csr_v[posInCSR]; nbrIdx < csrEntry->csr_v[posInCSR + 1];
             nbrIdx++) {
            auto nbrOffset = csrEntry->nbrNodeOffsets[nbrIdx];
            auto state = ifeMorsel->visitedNodes[nbrOffset];
            if (state == NOT_VISITED_DST) {
                if (state == __atomic_exchange_n(&ifeMorsel->visitedNodes[nbrOffset], VISITED_DST,
                                 __ATOMIC_ACQ_REL)) {
                    numDstVisitedLocal++;
                    __atomic_store_n(&ifeMorsel->pathLength[nbrOffset], ifeMorsel->currentLevel + 1,
                        __ATOMIC_RELAXED);
                    __atomic_store_n(&ifeMorsel->nextFrontier[nbrOffset], 1u, __ATOMIC_RELAXED);
                }
            } else if (state == NOT_VISITED) {
                if (state == __atomic_exchange_n(&ifeMorsel->visitedNodes[nbrOffset], VISITED,
                                 __ATOMIC_ACQ_REL)) {
                    numNonDstVisitedLocal++;
                    __atomic_store_n(&ifeMorsel->nextFrontier[nbrOffset], 1u, __ATOMIC_RELAXED);
                }
            }
        }
        return {numDstVisitedLocal, numNonDstVisitedLocal};
    }

    static void extendNode(graph::Graph* graph, IFEMorsel* ifeMorsel, const common::offset_t offset,
        uint64_t& numDstVisitedLocal, uint64_t& numNonDstVisitedLocal,
        graph::NbrScanState* nbrScanState) {
        std::pair<uint64_t, uint64_t> retVal;
        if (graph->isInMemory) {
            retVal = visitNbrs(offset, ifeMorsel, graph);
            numDstVisitedLocal += retVal.first;
            numNonDstVisitedLocal += retVal.second;
        } else {
            graph->initializeStateFwdNbrs(offset, nbrScanState);
            do {
                graph->getFwdNbrs(nbrScanState);
                retVal = visitNbrs(ifeMorsel, *nbrScanState->dstNodeIDVector.get());
                numDstVisitedLocal += retVal.first;
                numNonDstVisitedLocal += retVal.second;
            } while (graph->hasMoreFwdNbrs(nbrScanState));
        }
    }

    static uint64_t extendSparseFrontierFunc(GDSCallSharedState* sharedState,
        GDSLocalState* localState) {
        auto& graph = sharedState->graph;
        auto shortestPathLocalState =
            common::ku_dynamic_cast<GDSLocalState*, ParallelShortestPathLocalState*>(localState);
        auto ifeMorsel = shortestPathLocalState->ifeMorsel;
        auto morselSize = graph->isInMemory ? 512LU : 256LU;
        auto frontierMorsel = ifeMorsel->getMorsel(morselSize);
        if (!frontierMorsel.hasMoreToOutput()) {
            return 0; // return 0 to indicate to thread it can exit from operator
        }
        auto& nbrScanState = shortestPathLocalState->nbrScanState;
        uint64_t numDstVisitedLocal = 0u, numNonDstVisitedLocal = 0u;
        while (!ifeMorsel->isBFSCompleteNoLock() && frontierMorsel.hasMoreToOutput()) {
            for (auto idx = frontierMorsel.startOffset; idx < frontierMorsel.endOffset; idx++) {
                extendNode(graph.get(), ifeMorsel, ifeMorsel->bfsFrontier[idx], numDstVisitedLocal,
                    numNonDstVisitedLocal, nbrScanState.get());
            }
            frontierMorsel = ifeMorsel->getMorsel(morselSize);
        }
        ifeMorsel->mergeResults(numDstVisitedLocal, numNonDstVisitedLocal);
        return 0u;
    }

    static uint64_t extendDenseFrontierFunc(GDSCallSharedState* sharedState,
        GDSLocalState* localState) {
        auto& graph = sharedState->graph;
        auto shortestPathLocalState =
            common::ku_dynamic_cast<GDSLocalState*, ParallelShortestPathLocalState*>(localState);
        auto ifeMorsel = shortestPathLocalState->ifeMorsel;
        auto morselSize = graph->isInMemory ? 512LU : 256LU;
        auto frontierMorsel = ifeMorsel->getMorsel(morselSize);
        if (!frontierMorsel.hasMoreToOutput()) {
            return 0; // return 0 to indicate to thread it can exit from operator
        }
        auto& nbrScanState = shortestPathLocalState->nbrScanState;
        uint64_t numDstVisitedLocal = 0u, numNonDstVisitedLocal = 0u;
        while (!ifeMorsel->isBFSCompleteNoLock() && frontierMorsel.hasMoreToOutput()) {
            for (auto offset = frontierMorsel.startOffset; offset < frontierMorsel.endOffset;
                 offset++) {
                if (!ifeMorsel->currentFrontier[offset]) {
                    continue;
                }
                extendNode(graph.get(), ifeMorsel, offset, numDstVisitedLocal,
                    numNonDstVisitedLocal, nbrScanState.get());
            }
            frontierMorsel = ifeMorsel->getMorsel(morselSize);
        }
        ifeMorsel->mergeResults(numDstVisitedLocal, numNonDstVisitedLocal);
        return 0u;
    }

    static uint64_t shortestPathOutputFunc(GDSCallSharedState* sharedState,
        GDSLocalState* localState) {
        auto shortestPathLocalState =
            common::ku_dynamic_cast<GDSLocalState*, ParallelShortestPathLocalState*>(localState);
        auto ifeMorsel = shortestPathLocalState->ifeMorsel;
        auto morsel = ifeMorsel->getDstWriteMorsel(DEFAULT_VECTOR_CAPACITY);
        if (!morsel.hasMoreToOutput()) {
            return 0;
        }
        auto tableID = sharedState->graph->getNodeTableID();
        auto& srcNodeVector = shortestPathLocalState->srcNodeIDVector;
        auto& dstOffsetVector = shortestPathLocalState->dstNodeIDVector;
        auto& pathLengthVector = shortestPathLocalState->lengthVector;
        srcNodeVector->setValue<nodeID_t>(0, {ifeMorsel->srcOffset, tableID});
        auto pos = 0;
        for (auto offset = morsel.startOffset; offset < morsel.endOffset; offset++) {
            auto pathLength = ifeMorsel->pathLength[offset];
            if (pathLength >= ifeMorsel->lowerBound) {
                dstOffsetVector->setValue<nodeID_t>(pos, {offset, tableID});
                pathLengthVector->setValue<int64_t>(pos, pathLength);
                pos++;
            }
        }
        if (pos == 0) {
            return UINT64_MAX;
        }
        dstOffsetVector->state->getSelVectorUnsafe().setSelSize(pos);
        return pos; // return the no. of output values written to the value vectors
    }

    void exec(processor::ExecutionContext* executionContext) override {
        auto extraData = bindData->ptrCast<ParallelShortestPathBindData>();
        auto numNodes = sharedState->graph->getNumNodes();
        auto ifeMorsel = std::make_unique<IFEMorsel>(extraData->upperBound, 1, numNodes - 1,
            common::INVALID_OFFSET);
        auto& inputMask = sharedState->inputNodeOffsetMasks[sharedState->graph->getNodeTableID()];
        for (auto offset = 0u; offset < numNodes; offset++) {
            if (!inputMask->isMasked(offset)) {
                continue;
            }
            ifeMorsel->resetNoLock(offset);
            ifeMorsel->init();
            while (!ifeMorsel->isBFSCompleteNoLock()) {
                /*auto duration = std::chrono::system_clock::now().time_since_epoch();
                auto millis =
                    std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();*/
                printf("starting bfs level: %d\n", ifeMorsel->currentLevel);
                auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>();
                gdsLocalState->ifeMorsel = ifeMorsel.get();
                if (ifeMorsel->isSparseFrontier) {
                    auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                        sharedState, extendSparseFrontierFunc, true /* isParallel */};
                    parallelUtils->submitParallelTaskAndWait(job);
                } else {
                    auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                        sharedState, extendDenseFrontierFunc, true /* isParallel */};
                    parallelUtils->submitParallelTaskAndWait(job);
                }
                /*auto duration1 = std::chrono::system_clock::now().time_since_epoch();
                auto millis1 =
                    std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
                printf("bfs level: %d completed in %ld ms \n", ifeMorsel->currentLevel,
                    millis1 - millis);*/
                ifeMorsel->initializeNextFrontierNoLock();
            }
            /*auto duration = std::chrono::system_clock::now().time_since_epoch();
            auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();*/
            auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>();
            gdsLocalState->ifeMorsel = ifeMorsel.get();
            auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
                shortestPathOutputFunc, true /* isParallel */};
            parallelUtils->submitParallelTaskAndWait(job);
            /*auto duration1 = std::chrono::system_clock::now().time_since_epoch();
            auto millis1 = std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
            printf("output writing completed in %lu ms\n", millis1 - millis);*/
        }
    }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<nT1SParallelShortestPath>(*this);
    }
};

function_set nT1SParallelShortestPathsFunction::getFunctionSet() {
    function_set result;
    auto function =
        std::make_unique<GDSFunction>(name, std::make_unique<nT1SParallelShortestPath>());
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
