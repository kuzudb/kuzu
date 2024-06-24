#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
#include "common/types/types.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/ife_morsel.h"
#include "function/gds/parallel_shortest_path_commons.h"
#include "function/gds/parallel_utils.h"
#include "function/gds_function.h"
#include "main/settings.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace function {

class _1T1SParallelShortestPath : public GDSAlgorithm {
public:
    _1T1SParallelShortestPath() = default;
    _1T1SParallelShortestPath(const _1T1SParallelShortestPath& other) : GDSAlgorithm{other} {}

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
        columns.push_back(binder->createVariable("dst", *LogicalType::INTERNAL_ID()));
        columns.push_back(binder->createVariable("length", *LogicalType::INT64()));
        return columns;
    }

    void bind(const binder::expression_vector& params) override {
        KU_ASSERT(params.size() == 3);
        auto inputNode = params[1];
        ExpressionUtil::validateExpressionType(*params[2], ExpressionType::LITERAL);
        ExpressionUtil::validateDataType(*params[2], *LogicalType::INT64());
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
            auto state = ifeMorsel->visitedNodes[dstNodeID.offset].load(std::memory_order_acq_rel);
            if (state == NOT_VISITED_DST) {
                auto tryCAS = ifeMorsel->visitedNodes[dstNodeID.offset].compare_exchange_strong(
                    state, VISITED_DST_NEW, std::memory_order_acq_rel);
                if (tryCAS) {
                    numDstVisitedLocal++;
                    ifeMorsel->pathLength[dstNodeID.offset].store(ifeMorsel->currentLevel + 1,
                        std::memory_order_relaxed);
                }
            } else if (state == NOT_VISITED) {
                ifeMorsel->visitedNodes[dstNodeID.offset].store(VISITED_NEW,
                    std::memory_order_acq_rel);
            }
        }
        return numDstVisitedLocal;
    }

    // BFS Frontier Extension function, return only after bfs extension complete.
    static void extendFrontierFunc(GDSCallSharedState* sharedState, GDSLocalState* localState) {
        auto& graph = sharedState->graph;
        auto shortestPathLocalState =
            common::ku_dynamic_cast<GDSLocalState*, ParallelShortestPathLocalState*>(localState);
        auto ifeMorsel = shortestPathLocalState->ifeMorsel;
        ifeMorsel->init();
        while (!ifeMorsel->isBFSCompleteNoLock()) {
            auto numDstVisitedLocal = 0u;
            for (auto pos = 0u; pos < ifeMorsel->bfsLevelNodeOffsets.size(); pos++) {
                auto frontierOffset = ifeMorsel->bfsLevelNodeOffsets[pos];
                graph->initializeStateFwdNbrs(frontierOffset,
                    shortestPathLocalState->nbrScanState.get());
                do {
                    auto& dstNodeIDVector =
                        graph->getFwdNbrs(shortestPathLocalState->nbrScanState.get());
                    numDstVisitedLocal += visitNbrs(ifeMorsel, dstNodeIDVector);
                } while (graph->hasMoreFwdNbrs(shortestPathLocalState->nbrScanState.get()));
                ifeMorsel->mergeResults(numDstVisitedLocal);
            }
            ifeMorsel->initializeNextFrontierNoLock();
        }
    }

    static uint64_t shortestPathOutputFunc(GDSCallSharedState* sharedState,
        GDSLocalState* localState) {
        auto shortestPathLocalState =
            common::ku_dynamic_cast<GDSLocalState*, ParallelShortestPathLocalState*>(localState);
        auto ifeMorsel = shortestPathLocalState->ifeMorsel;
        if (ifeMorsel->nextDstScanStartIdx >= ifeMorsel->maxOffset) {
            return 0;
        }
        auto tableID = sharedState->graph->getNodeTableID();
        auto& srcNodeVector = shortestPathLocalState->srcNodeIDVector;
        auto& dstOffsetVector = shortestPathLocalState->dstNodeIDVector;
        auto& pathLengthVector = shortestPathLocalState->lengthVector;
        srcNodeVector->setValue<nodeID_t>(0, {ifeMorsel->srcOffset, tableID});
        auto pos = 0LU, offset = ifeMorsel->nextDstScanStartIdx.load(std::memory_order_relaxed);
        while (pos < DEFAULT_VECTOR_CAPACITY && offset <= ifeMorsel->maxOffset) {
            auto state = ifeMorsel->visitedNodes[offset].load(std::memory_order_relaxed);
            uint64_t pathLength = ifeMorsel->pathLength[offset].load(std::memory_order_acq_rel);
            if ((state == VISITED_DST_NEW || state == VISITED_DST) &&
                pathLength >= ifeMorsel->lowerBound) {
                dstOffsetVector->setValue<nodeID_t>(pos, {offset, tableID});
                pathLengthVector->setValue<uint64_t>(pos, pathLength);
                pos++;
            }
            offset++;
        }
        dstOffsetVector->state->getSelVectorUnsafe().setSelSize(pos);
        ifeMorsel->nextDstScanStartIdx.store(offset, std::memory_order_relaxed);
        return pos; // return the no. of output values written to the value vectors
    }

    static uint64_t mainFunc(GDSCallSharedState* sharedState, GDSLocalState* localState) {
        auto shortestPathLocalState =
            common::ku_dynamic_cast<GDSLocalState*, ParallelShortestPathLocalState*>(localState);
        if (shortestPathLocalState->ifeMorsel->isBFSCompleteNoLock()) {
            return shortestPathOutputFunc(sharedState, localState);
        }
        extendFrontierFunc(sharedState, localState);
        return shortestPathOutputFunc(sharedState, localState);
    }

    void exec(processor::ExecutionContext* executionContext) override {
        auto extraData = bindData->ptrCast<ParallelShortestPathBindData>();
        // threads available will be 1 less than total (main thread makes gds call)
        auto threadsAvailable = executionContext->clientContext->getClientConfig()->numThreads - 1;
        auto concurrentBFS = executionContext->clientContext->getClientConfig()->maxConcurrentBFS;
        // set max bfs always to min value between threads available and maxConcurrentBFS value
        auto maxConcurrentBFS = std::min(threadsAvailable, concurrentBFS);
        auto maxNodeOffset = sharedState->graph->getNumNodes() - 1;
        auto lowerBound = 1u;
        auto& inputMask = sharedState->inputNodeOffsetMask;
        scheduledTaskMap ifeMorselTasks;
        ifeMorselTasks.reserve(maxConcurrentBFS);
        auto srcOffset = 0LU, numCompletedTasks = 0LU, totalBFSSources = 0LU;
        while (true) {
            for (auto i = 0u; i < maxConcurrentBFS; i++) {
                if (!ifeMorselTasks[i].first) {
                    // set up new ife_morsel with srcOffset (if node masked)
                    // set up task with pointer to the function to execute
                    while (srcOffset <= maxNodeOffset) {
                        if (inputMask->isNodeMasked(srcOffset)) {
                            break;
                        }
                        srcOffset++;
                    }
                    if ((srcOffset > maxNodeOffset) && (totalBFSSources == numCompletedTasks)) {
                        break;
                    }
                    totalBFSSources++;
                    ifeMorselTasks[i].first = std::make_unique<IFEMorsel>(extraData->upperBound,
                        lowerBound, maxNodeOffset, srcOffset);
                    auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>();
                    gdsLocalState->ifeMorsel = ifeMorselTasks[i].first.get();
                    auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                        sharedState, mainFunc, false /* isParallel */};
                    ifeMorselTasks[i].second = parallelUtils->submitTaskAndReturn(job);
                } else if (parallelUtils->taskCompletedNoError(ifeMorselTasks[i].second)) {
                    numCompletedTasks++;
                    while (srcOffset <= maxNodeOffset) {
                        if (inputMask->isNodeMasked(srcOffset)) {
                            break;
                        }
                        srcOffset++;
                    }
                    if ((srcOffset > maxNodeOffset) && (totalBFSSources == numCompletedTasks)) {
                        break;
                    }
                    totalBFSSources++;
                    ifeMorselTasks[i].first->resetNoLock(srcOffset);
                    auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>();
                    gdsLocalState->ifeMorsel = ifeMorselTasks[i].first.get();
                    auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                        sharedState, mainFunc, false /* isParallel */};
                    ifeMorselTasks[i].second = parallelUtils->submitTaskAndReturn(job);
                } else if (parallelUtils->taskHasExceptionOrTimedOut(ifeMorselTasks[i].second,
                               executionContext)) {
                    // Can we exit from here ? Or should we remove all the other remaining tasks ?
                    break;
                } else {
                    // Else the task is still running, check status of the other tasks.
                    continue;
                }
            }
            std::this_thread::sleep_for(
                std::chrono::microseconds(THREAD_SLEEP_TIME_WHEN_WAITING_IN_MICROS));
        }
    }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<_1T1SParallelShortestPath>(*this);
    }
};

function_set _1T1SParallelShortestPathFunction::getFunctionSet() {
    function_set result;
    auto function =
        std::make_unique<GDSFunction>(name, std::make_unique<_1T1SParallelShortestPath>());
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
