#include "src/processor/include/physical_plan/operator/order_by/order_by_merge.h"

#include "src/common/include/configs.h"

namespace graphflow {
namespace processor {

shared_ptr<ResultSet> OrderByMerge::initResultSet() {
    // OrderByMerge is the only sink operator in a pipeline and only modifies the
    // sharedState by merging sortedKeyBlocks, So we don't need to initialize the resultSet.
    keyBlockMerger =
        make_unique<KeyBlockMerger>(sharedRowCollectionsAndSortedKeyBlocks->rowCollections,
            sharedRowCollectionsAndSortedKeyBlocks->strKeyColInfo,
            sharedRowCollectionsAndSortedKeyBlocks->keyBlockEntrySizeInBytes);
    return resultSet;
}

void OrderByMerge::execute() {
    keyBlockMergeTaskDispatcher->initIfNecessary(context.memoryManager,
        sharedRowCollectionsAndSortedKeyBlocks->sortedKeyBlocks,
        sharedRowCollectionsAndSortedKeyBlocks->rowCollections,
        sharedRowCollectionsAndSortedKeyBlocks->strKeyColInfo,
        sharedRowCollectionsAndSortedKeyBlocks->keyBlockEntrySizeInBytes);
    metrics->executionTime.start();
    Sink::execute();
    while (!keyBlockMergeTaskDispatcher->isDoneMerge()) {
        auto keyBlockMergeMorsel = keyBlockMergeTaskDispatcher->getMorsel();
        if (keyBlockMergeMorsel == nullptr) {
            this_thread::sleep_for(chrono::microseconds(THREAD_SLEEP_TIME_WHEN_WAITING_IN_MICROS));
            continue;
        }
        keyBlockMerger->mergeKeyBlocks(*keyBlockMergeMorsel);
        keyBlockMergeTaskDispatcher->doneMorsel(move(keyBlockMergeMorsel));
    }
    metrics->executionTime.stop();
}

void OrderByMerge::finalize() {}

} // namespace processor
} // namespace graphflow
