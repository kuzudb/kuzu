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
#include "processor/processor_task.h"

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
            auto state = ifeMorsel->visitedNodes[dstNodeID.offset].load(std::memory_order_acq_rel);
            if (state == NOT_VISITED_DST) {
                auto tryCAS = ifeMorsel->visitedNodes[dstNodeID.offset].compare_exchange_strong(
                    state, VISITED_DST_NEW, std::memory_order_acq_rel);
                if (tryCAS) {
                    numDstVisitedLocal++;
                    ifeMorsel->pathLength[dstNodeID.offset].store(ifeMorsel->currentLevel + 1,
                        std::memory_order_acq_rel);
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
        while (!ifeMorsel->isBFSCompleteNoLock()) {
            auto numDstVisitedLocal = 0u;
            /*auto duration = std::chrono::system_clock::now().time_since_epoch();
            auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();*/
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
            /*auto duration1 = std::chrono::system_clock::now().time_since_epoch();
            auto millis1 = std::chrono::duration_cast<std::chrono::milliseconds>(duration1).count();
            printf("bfs source: %lu, level: %d extension done in %lu ms\n", ifeMorsel->srcOffset,
                ifeMorsel->currentLevel, millis1 - millis);*/
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
        auto pos = 0LU, offset = ifeMorsel->nextDstScanStartIdx.load(std::memory_order_acq_rel);
        while (pos < DEFAULT_VECTOR_CAPACITY && offset <= ifeMorsel->maxOffset) {
            auto state = ifeMorsel->visitedNodes[offset].load(std::memory_order_acq_rel);
            auto pathLength = ifeMorsel->pathLength[offset].load(std::memory_order_acq_rel);
            if ((state == VISITED_DST_NEW || state == VISITED_DST) &&
                pathLength >= ifeMorsel->lowerBound) {
                dstOffsetVector->setValue<nodeID_t>(pos, {offset, tableID});
                pathLengthVector->setValue<int64_t>(pos, pathLength);
                pos++;
            }
            offset++;
        }
        ifeMorsel->nextDstScanStartIdx.store(offset, std::memory_order_acq_rel);
        if (pos == 0) {
            return UINT64_MAX;
        }
        dstOffsetVector->state->getSelVectorUnsafe().setSelSize(pos);
        return pos; // return the no. of output values written to the value vectors
    }

    static uint64_t mainFunc(GDSCallSharedState* sharedState, GDSLocalState* localState) {
        auto shortestPathLocalState =
            common::ku_dynamic_cast<GDSLocalState*, ParallelShortestPathLocalState*>(localState);
        shortestPathLocalState->ifeMorsel->init();
        if (shortestPathLocalState->ifeMorsel->isBFSCompleteNoLock()) {
            return shortestPathOutputFunc(sharedState, localState);
        }
        extendFrontierFunc(sharedState, localState);
        /*auto duration = std::chrono::system_clock::now().time_since_epoch();
        auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
        printf("starting output writing for src: %lu at time: %lu\n",
            shortestPathLocalState->ifeMorsel->srcOffset, millis);*/
        return shortestPathOutputFunc(sharedState, localState);
    }

    void exec(processor::ExecutionContext* executionContext) override {
        auto extraData = bindData->ptrCast<ParallelShortestPathBindData>();
        // threads available will be 1 less than total (main thread makes gds call)
        auto concurrentBFS = executionContext->clientContext->getClientConfig()->maxConcurrentBFS;
        // set max bfs always to min value between threads available and maxConcurrentBFS value
        auto maxConcurrentBFS = std::max(1LU, concurrentBFS);
        printf("max concurrent bfs setting: %lu, launching maxConcurrentBFS as: %lu\n",
            concurrentBFS, maxConcurrentBFS);
        auto maxNodeOffset = sharedState->graph->getNumNodes() - 1;
        auto lowerBound = 1u;
        auto& inputMask = sharedState->inputNodeOffsetMasks[0];
        scheduledTaskMap ifeMorselTasks = scheduledTaskMap();
        auto srcOffset = 0LU, numCompletedTasks = 0LU, totalBFSSources = 0LU;
        /*
         * We need to seed `maxConcurrentBFS` no. of tasks into the queue first. And then we reuse
         * the IFEMorsels initialized again and again for further tasks.
         * (1) Prepare at most maxConcurrentBFS no. of IFEMorsels as tasks to push into queue
         * (2) If we reach maxConcurrentBFS before reaching end of total nodes, then break.
         * (3) If we reach total nodes before we hit maxConcurrentBFS, then break.
         */
        while (totalBFSSources < maxConcurrentBFS) {
            while (srcOffset <= maxNodeOffset) {
                if (inputMask->isMasked(srcOffset)) {
                    break;
                }
                srcOffset++;
            }
            if (srcOffset > maxNodeOffset) {
                break;
            }
            totalBFSSources++;
            /*auto duration = std::chrono::system_clock::now().time_since_epoch();
            auto millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
            printf("starting bfs source: %lu at %lu\n", srcOffset, millis);*/
            auto ifeMorsel = std::make_unique<IFEMorsel>(extraData->upperBound, lowerBound,
                maxNodeOffset, srcOffset);
            srcOffset++;
            auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>();
            gdsLocalState->ifeMorsel = ifeMorsel.get();
            auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
                mainFunc, false /* isParallel */};
            auto scheduledTask = parallelUtils->submitTaskAndReturn(job);
            // printf("submitted job for bfs source: %lu\n", ifeMorsel->srcOffset);
            ifeMorselTasks.push_back({std::move(ifeMorsel), scheduledTask});
        }
        if (ifeMorselTasks.empty()) {
            return;
        }
        bool runLoop = true;
        while (runLoop) {
            for (auto i = 0u; i < ifeMorselTasks.size(); i++) {
                auto& schedTask = ifeMorselTasks[i].second;
                if (!schedTask) {
                    continue;
                }
                if (!parallelUtils->taskCompletedNoError(schedTask)) {
                    continue;
                }
                if (parallelUtils->taskHasExceptionOrTimedOut(schedTask, executionContext)) {
                    // Can we exit from here ? Or should we remove all the other remaining tasks ?
                    // TODO: Handling errors is not currently fixed, all tasks should be removed
                    runLoop = false;
                    break;
                }
                /*auto duration = std::chrono::system_clock::now().time_since_epoch();
                auto millis =
                    std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
                printf("bfs source: %lu finished at time: %lu\n",
                    ifeMorselTasks[i].first->srcOffset, millis);*/
                auto processorTask = common::ku_dynamic_cast<Task*, ProcessorTask*>(
                    ifeMorselTasks[i].second->task.get());
                free(processorTask->getSink());
                ifeMorselTasks[i].second = nullptr;
                // printf("bfs source: %lu is completed\n", ifeMorselTasks[i].first->srcOffset);
                numCompletedTasks++;
                while (srcOffset <= maxNodeOffset) {
                    if (inputMask->isMasked(srcOffset)) {
                        break;
                    }
                    srcOffset++;
                }
                if ((srcOffset > maxNodeOffset) && (totalBFSSources == numCompletedTasks)) {
                    return; // reached termination, all bfs sources launched have finished
                } else if (srcOffset > maxNodeOffset) {
                    continue;
                }
                /*duration = std::chrono::system_clock::now().time_since_epoch();
                millis = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
                printf("bfs source: %lu starting at time: %lu\n", srcOffset, millis);*/
                totalBFSSources++;
                ifeMorselTasks[i].first->resetNoLock(srcOffset);
                srcOffset++;
                auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>();
                gdsLocalState->ifeMorsel = ifeMorselTasks[i].first.get();
                auto job = ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
                    mainFunc, false /* isParallel */};
                ifeMorselTasks[i].second = parallelUtils->submitTaskAndReturn(job);
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
