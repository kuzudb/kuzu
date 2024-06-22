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

typedef std::vector<std::pair<std::shared_ptr<IFEMorsel>&, std::shared_ptr<ScheduledTask>&>>
    scheduledTaskMap;

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
        localState = std::make_unique<ParallelShortestPathLocalState>(context);
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

    /*
     * This will be main function for logic, doing frontier extension and updating state.
     */
    static uint64_t extendFrontierFunc(GDSCallSharedState* sharedState, GDSLocalState* localState) {

    }

    static uint64_t shortestPathOutputFunc(GDSCallSharedState* sharedState,
        GDSLocalState* localState) {}

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
        auto srcOffset = 0LU, numCompletedTasks = 0lu, totalBFSSources = 0lu;
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
                    if ((srcOffset > maxNodeOffset) &&
                        (totalBFSSources == numCompletedTasks)) {
                        break;
                    }
                    totalBFSSources++;
                    ifeMorselTasks[i].first =
                        std::make_unique<IFEMorsel>(extraData->upperBound, lowerBound, maxNodeOffset);
                    ifeMorselTasks[i].first->initSourceNoLock(srcOffset);
                    ifeMorselTasks[i].second = parallelUtils->doSequentialNonBlocking(
                        executionContext, this, sharedState, extendFrontierFunc);
                } else if (parallelUtils->taskCompletedNoError(ifeMorselTasks[i].second)) {
                    numCompletedTasks++;
                    while (srcOffset <= maxNodeOffset) {
                        if (inputMask->isNodeMasked(srcOffset)) {
                            break;
                        }
                        srcOffset++;
                    }
                    if ((srcOffset > maxNodeOffset) &&
                        (totalBFSSources == numCompletedTasks)) {
                        break;
                    }
                    ifeMorselTasks[i].first->initSourceNoLock(srcOffset);
                    ifeMorselTasks[i].second = parallelUtils->doSequentialNonBlocking(
                        executionContext, this, sharedState, extendFrontierFunc);
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
