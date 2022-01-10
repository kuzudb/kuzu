#pragma once

#include <queue>

#include "src/common/include/data_chunk/data_chunk_state.h"
#include "src/common/include/types.h"
#include "src/common/include/vector/string_buffer.h"
#include "src/processor/include/physical_plan/operator/morsel.h"
#include "src/processor/include/physical_plan/operator/order_by/radix_sort.h"
#include "src/processor/include/physical_plan/operator/sink.h"
#include "src/processor/include/physical_plan/result/result_set.h"
#include "src/processor/include/physical_plan/result/row_collection.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace processor {

// This class contains rowCollections, nextRowCollectionID, strColInfo, sortedKeyBlocks and
// the size of each row in keyBlocks. The class is shared between the orderBy,
// orderByMerge, orderByScan operators. All functions are guaranteed to be thread-safe,
// so caller doesn't need to acquire a lock before calling these functions.
class SharedRowCollectionsAndSortedKeyBlocks {
public:
    explicit SharedRowCollectionsAndSortedKeyBlocks()
        : nextRowCollectionIdx{0}, sortedKeyBlocks{make_shared<queue<shared_ptr<KeyBlock>>>()} {}

    uint16_t getNextRowCollectionIdx() {
        lock_guard<mutex> sharedStateLock{orderBySharedStateLock};
        return nextRowCollectionIdx++;
    }

    void appendRowCollection(uint16_t rowCollectionIdx, shared_ptr<RowCollection> rowCollection) {
        lock_guard<mutex> sharedStateLock{orderBySharedStateLock};
        // If the rowCollections is full, resize the rowCollections and
        // insert the rowCollection to the set.
        if (rowCollectionIdx >= rowCollections.size()) {
            rowCollections.resize(rowCollectionIdx + 1);
        }
        rowCollections[rowCollectionIdx] = move(rowCollection);
    }

    void appendSortedKeyBlock(shared_ptr<KeyBlock> keyBlock) {
        lock_guard<mutex> sharedStateLock{orderBySharedStateLock};
        sortedKeyBlocks->emplace(keyBlock);
    }

    void setKeyBlockEntrySizeInBytes(uint64_t keyBlockEntrySizeInBytes) {
        lock_guard<mutex> sharedStateLock{orderBySharedStateLock};
        this->keyBlockEntrySizeInBytes = keyBlockEntrySizeInBytes;
    }

    void setStrKeyColInfo(vector<StrKeyColInfo>& strKeyColInfo) {
        lock_guard<mutex> sharedStateLock{orderBySharedStateLock};
        this->strKeyColInfo = move(strKeyColInfo);
    }

private:
    mutex orderBySharedStateLock;

public:
    vector<shared_ptr<RowCollection>> rowCollections;
    uint16_t nextRowCollectionIdx;
    shared_ptr<queue<shared_ptr<KeyBlock>>> sortedKeyBlocks;

    uint64_t keyBlockEntrySizeInBytes;
    vector<StrKeyColInfo> strKeyColInfo;
};

struct OrderByDataInfo {

public:
    OrderByDataInfo(vector<DataPos> keyDataPoses, vector<DataPos> allDataPoses,
        vector<bool> isVectorFlat, vector<bool> isAscOrder)
        : keyDataPoses{move(keyDataPoses)}, allDataPoses{move(allDataPoses)},
          isVectorFlat{move(isVectorFlat)}, isAscOrder{move(isAscOrder)} {}

    OrderByDataInfo(const OrderByDataInfo& other)
        : OrderByDataInfo{
              other.keyDataPoses, other.allDataPoses, other.isVectorFlat, other.isAscOrder} {}

public:
    vector<DataPos> keyDataPoses;
    vector<DataPos> allDataPoses;
    vector<bool> isVectorFlat;
    vector<bool> isAscOrder;
};

class OrderBy : public Sink {
public:
    OrderBy(const OrderByDataInfo& orderByDataInfo,
        shared_ptr<SharedRowCollectionsAndSortedKeyBlocks> sharedState,
        unique_ptr<PhysicalOperator> child, ExecutionContext& context, uint32_t id)
        : Sink{move(child), context, id}, orderByDataInfo{orderByDataInfo}, sharedState{
                                                                                sharedState} {}

    shared_ptr<ResultSet> initResultSet() override;
    void execute() override;
    void finalize() override;

    PhysicalOperatorType getOperatorType() override { return ORDER_BY; }

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<OrderBy>(
            orderByDataInfo, sharedState, children[0]->clone(), context, id);
    }

private:
    uint16_t rowCollectionIdx;
    OrderByDataInfo orderByDataInfo;
    unique_ptr<OrderByKeyEncoder> orderByKeyEncoder;
    unique_ptr<RadixSort> radixSorter;
    vector<shared_ptr<ValueVector>> keyVectors;
    vector<shared_ptr<ValueVector>> vectorsToAppend;
    vector<StrKeyColInfo> strKeyColInfo;
    shared_ptr<SharedRowCollectionsAndSortedKeyBlocks> sharedState;
    shared_ptr<RowCollection> localRowCollection;
};

} // namespace processor
} // namespace graphflow
