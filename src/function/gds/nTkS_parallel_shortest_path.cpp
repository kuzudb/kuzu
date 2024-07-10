#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
#include "common/types/types.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds/ife_morsel.h"
#include "function/gds/parallel_shortest_path_commons.h"
#include "function/gds/parallel_utils.h"
#include "function/gds_function.h"
#include "processor/processor_task.h"
#include "graph/in_mem_graph.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace function {

class nTkSParallelShortestPath : public GDSAlgorithm {
public:
    nTkSParallelShortestPath() = default;
    nTkSParallelShortestPath(const nTkSParallelShortestPath& other) : GDSAlgorithm{other} {}

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
        if (!ifeMorsel->initializedIFEMorsel) {
            ifeMorsel->init();
        }
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
        srcNodeVector->setValue<nodeID_t>(0, common::nodeID_t{ifeMorsel->srcOffset, tableID});
        auto pos = 0;
        for (auto offset = morsel.startOffset; offset < morsel.endOffset; offset++) {
            auto state = ifeMorsel->visitedNodes[offset];
            uint64_t pathLength = ifeMorsel->pathLength[offset];
            if ((state == VISITED_DST_NEW || state == VISITED_DST) &&
                pathLength >= ifeMorsel->lowerBound) {
                dstOffsetVector->setValue<nodeID_t>(pos, common::nodeID_t{offset, tableID});
                pathLengthVector->setValue<uint64_t>(pos, pathLength);
                pos++;
            }
        }
        if (pos == 0) {
            return shortestPathOutputFunc(sharedState, localState);
        }
        dstOffsetVector->state->getSelVectorUnsafe().setSelSize(pos);
        return pos; // return the no. of output values written to the value vectors
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
        auto& inputMask = sharedState->inputNodeOffsetMasks[sharedState->graph->getNodeTableID()];
        scheduledTaskMap ifeMorselTasks = scheduledTaskMap();
        std::vector<ParallelUtilsJob> jobs; // stores the next batch of jobs to submit
        std::vector<int> jobIdxInMap;       // stores the scheduledTaskMap idx <-> job mapping
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
            // printf("starting bfs source: %lu\n", srcOffset);
            auto ifeMorsel = std::make_unique<IFEMorsel>(extraData->upperBound, lowerBound,
                maxNodeOffset, srcOffset);
            srcOffset++;
            auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>();
            gdsLocalState->ifeMorsel = ifeMorsel.get();
            jobs.push_back(ParallelUtilsJob{executionContext, std::move(gdsLocalState), sharedState,
                extendFrontierFunc, true /* isParallel */});
            ifeMorselTasks.push_back({std::move(ifeMorsel), nullptr});
        }
        auto scheduledTasks = parallelUtils->submitTasksAndReturn(jobs);
        // place the right scheduled task corresponding to its ife morsel
        for (auto i = 0u; i < scheduledTasks.size(); i++) {
            ifeMorselTasks[i].second = scheduledTasks[i];
            // printf("task ID: %lu was submitted for src: %lu\n", scheduledTasks[i]->ID,
            // ifeMorselTasks[i].first->srcOffset);
        }
        jobs.clear();
        jobIdxInMap.clear();
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
                if (ifeMorselTasks[i].first->isIFEMorselCompleteNoLock()) {
                    auto processorTask = common::ku_dynamic_cast<Task*, ProcessorTask*>(
                        ifeMorselTasks[i].second->task.get());
                    free(processorTask->getSink());
                    ifeMorselTasks[i].second = nullptr;
                    numCompletedTasks++;
                    // printf("bfs source: %lu is completed\n", ifeMorselTasks[i].first->srcOffset);
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
                    totalBFSSources++;
                    ifeMorselTasks[i].first->resetNoLock(srcOffset);
                    srcOffset++;
                    auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>();
                    gdsLocalState->ifeMorsel = ifeMorselTasks[i].first.get();
                    jobs.push_back(ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                        sharedState, extendFrontierFunc, true /* isParallel */});
                    jobIdxInMap.push_back(i);
                    continue;
                }
                bool isBFSComplete = ifeMorselTasks[i].first->isBFSCompleteNoLock();
                if (isBFSComplete) {
                    auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>();
                    gdsLocalState->ifeMorsel = ifeMorselTasks[i].first.get();
                    jobs.push_back(ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                        sharedState, shortestPathOutputFunc, true /* isParallel */});
                    jobIdxInMap.push_back(i);
                    continue;
                } else {
                    ifeMorselTasks[i].first->initializeNextFrontierNoLock();
                    auto gdsLocalState = std::make_unique<ParallelShortestPathLocalState>();
                    gdsLocalState->ifeMorsel = ifeMorselTasks[i].first.get();
                    jobs.push_back(ParallelUtilsJob{executionContext, std::move(gdsLocalState),
                        sharedState, extendFrontierFunc, true /* isParallel */});
                    jobIdxInMap.push_back(i);
                }
            }
            scheduledTasks = parallelUtils->submitTasksAndReturn(jobs);
            // place the right scheduled task corresponding to its ife morsel
            for (auto i = 0u; i < jobIdxInMap.size(); i++) {
                ifeMorselTasks[jobIdxInMap[i]].second = scheduledTasks[i];
            }
            jobs.clear();
            jobIdxInMap.clear();
            std::this_thread::sleep_for(
                std::chrono::microseconds(THREAD_SLEEP_TIME_WHEN_WAITING_IN_MICROS));
        }
    }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<nTkSParallelShortestPath>(*this);
    }
};

function_set nTkSParallelShortestPathsFunction::getFunctionSet() {
    function_set result;
    auto function =
        std::make_unique<GDSFunction>(name, std::make_unique<nTkSParallelShortestPath>());
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
