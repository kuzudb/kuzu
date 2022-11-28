#include "processor/operator/order_by/order_by_merge.h"

#include "common/configs.h"

namespace kuzu {
namespace processor {

shared_ptr<ResultSet> OrderByMerge::init(ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    // OrderByMerge is the only sink operator in a pipeline and only modifies the
    // sharedState by merging sortedKeyBlocks, So we don't need to initialize the resultSet.
    localMerger = make_unique<KeyBlockMerger>(
        sharedState->factorizedTables, sharedState->strKeyColsInfo, sharedState->numBytesPerTuple);
    return resultSet;
}

void OrderByMerge::executeInternal(ExecutionContext* context) {
    while (!sharedDispatcher->isDoneMerge()) {
        auto keyBlockMergeMorsel = sharedDispatcher->getMorsel();
        if (keyBlockMergeMorsel == nullptr) {
            this_thread::sleep_for(chrono::microseconds(THREAD_SLEEP_TIME_WHEN_WAITING_IN_MICROS));
            continue;
        }
        localMerger->mergeKeyBlocks(*keyBlockMergeMorsel);
        sharedDispatcher->doneMorsel(move(keyBlockMergeMorsel));
    }
}

void OrderByMerge::initGlobalStateInternal(ExecutionContext* context) {
    // TODO(Ziyi): directly feed sharedState to merger and dispatcher.
    sharedDispatcher->init(context->memoryManager, sharedState->sortedKeyBlocks,
        sharedState->factorizedTables, sharedState->strKeyColsInfo, sharedState->numBytesPerTuple);
}

} // namespace processor
} // namespace kuzu
