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

    static uint64_t visitNbrs(IFEMorsel* ifeMorsel, ValueVector& dstNodeIDVector) {
        uint64_t numDstVisitedLocal = 0u;
        for (auto j = 0u; j < dstNodeIDVector.state->getSelVector().getSelSize(); j++) {
            auto pos = dstNodeIDVector.state->getSelVector().operator[](j);
            auto dstNodeID = dstNodeIDVector.getValue<common::nodeID_t>(pos);
            auto state = ifeMorsel->visitedNodes[dstNodeID.offset];
            if (state == NOT_VISITED_DST) {
                if (__sync_bool_compare_and_swap(&ifeMorsel->visitedNodes[dstNodeID.offset], state,
                        VISITED_DST_NEW)) {
                    numDstVisitedLocal++;
                    __atomic_store_n(&ifeMorsel->pathLength[dstNodeID.offset],
                        ifeMorsel->currentLevel + 1, __ATOMIC_RELAXED);
                }
            } else if (state == NOT_VISITED) {
                __atomic_store_n(&ifeMorsel->visitedNodes[dstNodeID.offset], VISITED_NEW,
                    __ATOMIC_RELEASE);
            }
        }
        return numDstVisitedLocal;
    }

    static uint64_t visitNbrs(common::offset_t frontierOffset, IFEMorsel* ifeMorsel,
        graph::Graph* graph) {
        uint64_t numDstVisitedLocal = 0u;
        auto inMemGraph = ku_dynamic_cast<graph::Graph*, graph::InMemGraph*>(graph);
        auto& csr = inMemGraph->getInMemCSR();
        auto csrEntry = csr[frontierOffset >> RIGHT_SHIFT];
        if (!csrEntry) {
            return 0;
        }
        auto posInCSR = frontierOffset & OFFSET_DIV;
        for (auto nbrIdx = csrEntry->csr_v[posInCSR]; nbrIdx < csrEntry->csr_v[posInCSR + 1];
             nbrIdx++) {
            auto nbrOffset = csrEntry->nbrNodeOffsets[nbrIdx];
            auto state = ifeMorsel->visitedNodes[nbrOffset];
            if (state == NOT_VISITED_DST) {
                if (__sync_bool_compare_and_swap(&ifeMorsel->visitedNodes[nbrOffset], state,
                        VISITED_DST_NEW)) {
                    numDstVisitedLocal++;
                    __atomic_store_n(&ifeMorsel->pathLength[nbrOffset], ifeMorsel->currentLevel + 1,
                        __ATOMIC_RELAXED);
                }
            } else if (state == NOT_VISITED) {
                __atomic_store_n(&ifeMorsel->visitedNodes[nbrOffset], VISITED_NEW,
                    __ATOMIC_RELEASE);
            }
        }
        return numDstVisitedLocal;
    }

    /*
     * This will be main function for logic, doing frontier extension and updating state.
     */
    static uint64_t extendFrontierFunc(GDSCallSharedState* sharedState, GDSLocalState* localState) {
        auto& graph = sharedState->graph;
        auto shortestPathLocalState =
            common::ku_dynamic_cast<GDSLocalState*, ParallelShortestPathLocalState*>(localState);
        auto ifeMorsel = shortestPathLocalState->ifeMorsel;
        auto frontierMorsel = ifeMorsel->getMorsel(64LU /* morsel size, hard-coding for now */);
        if (!frontierMorsel.hasMoreToOutput()) {
            return 0; // return 0 to indicate to thread it can exit from operator
        }
        uint64_t numDstVisitedLocal = 0u;
        while (!ifeMorsel->isBFSCompleteNoLock() && frontierMorsel.hasMoreToOutput()) {
            for (auto i = frontierMorsel.startOffset; i < frontierMorsel.endOffset; i++) {
                auto frontierOffset = ifeMorsel->bfsLevelNodeOffsets[i];
                if (graph->isInMemory) {
                    numDstVisitedLocal += visitNbrs(frontierOffset, ifeMorsel, graph.get());
                } else {
                    graph->initializeStateFwdNbrs(frontierOffset,
                        shortestPathLocalState->nbrScanState.get());
                    do {
                        auto& dstNodeIDVector =
                            graph->getFwdNbrs(shortestPathLocalState->nbrScanState.get());
                        numDstVisitedLocal += visitNbrs(ifeMorsel, dstNodeIDVector);
                    } while (graph->hasMoreFwdNbrs(shortestPathLocalState->nbrScanState.get()));
                }
            }
            ifeMorsel->mergeResults(numDstVisitedLocal);
            frontierMorsel = ifeMorsel->getMorsel(64LU);
        }
        return UINT64_MAX; // returning UINT64_MAX to indicate to thread it should continue
                           // executing
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
            auto state = ifeMorsel->visitedNodes[offset];
            auto pathLength = ifeMorsel->pathLength[offset];
            if ((state == VISITED_DST_NEW || state == VISITED_DST) &&
                pathLength >= ifeMorsel->lowerBound) {
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
                auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
                    extendFrontierFunc, true /* isParallel */};
                parallelUtils->submitParallelTaskAndWait(job);
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
