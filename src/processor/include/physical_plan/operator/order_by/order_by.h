#pragma once

#include <queue>

#include "src/common/include/data_chunk/data_chunk_state.h"
#include "src/common/include/overflow_buffer.h"
#include "src/processor/include/physical_plan/operator/order_by/radix_sort.h"
#include "src/processor/include/physical_plan/operator/sink.h"
#include "src/processor/include/physical_plan/result/factorized_table.h"
#include "src/processor/include/physical_plan/result/result_set.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace processor {

// This class contains factorizedTables, nextFactorizedTableIdx, stringAndUnstructuredKeyColInfo,
// sortedKeyBlocks and the size of each tuple in keyBlocks. The class is shared between the orderBy,
// orderByMerge, orderByScan operators. All functions are guaranteed to be thread-safe,
// so caller doesn't need to acquire a lock before calling these functions.
class SharedFactorizedTablesAndSortedKeyBlocks {
public:
    explicit SharedFactorizedTablesAndSortedKeyBlocks()
        : nextFactorizedTableIdx{0},
          sortedKeyBlocks{make_shared<queue<shared_ptr<MergedKeyBlocks>>>()}, numBytesPerTuple{0} {}

    uint16_t getNextFactorizedTableIdx() {
        lock_guard<mutex> lck{orderBySharedStateMutex};
        return nextFactorizedTableIdx++;
    }

    void appendFactorizedTable(
        uint16_t factorizedTableIdx, shared_ptr<FactorizedTable> factorizedTable) {
        lock_guard<mutex> lck{orderBySharedStateMutex};
        // If the factorizedTables is full, resize the factorizedTables and
        // insert the factorizedTable to the set.
        if (factorizedTableIdx >= factorizedTables.size()) {
            factorizedTables.resize(factorizedTableIdx + 1);
        }
        factorizedTables[factorizedTableIdx] = move(factorizedTable);
    }

    void appendSortedKeyBlock(shared_ptr<MergedKeyBlocks> mergedDataBlocks) {
        lock_guard<mutex> lck{orderBySharedStateMutex};
        sortedKeyBlocks->emplace(move(mergedDataBlocks));
    }

    void setNumBytesPerTuple(uint64_t numBytesPerTupleToSet) {
        lock_guard<mutex> lck{orderBySharedStateMutex};
        this->numBytesPerTuple = numBytesPerTupleToSet;
    }

    void setStringAndUnstructuredKeyColInfo(
        vector<StringAndUnstructuredKeyColInfo>& stringAndUnstructuredKeyColInfoToSet) {
        lock_guard<mutex> lck{orderBySharedStateMutex};
        this->stringAndUnstructuredKeyColInfo = move(stringAndUnstructuredKeyColInfoToSet);
    }

    void setDataTypes(vector<DataType> dataTypesToSet) {
        lock_guard<mutex> lck{orderBySharedStateMutex};
        this->dataTypes = move(dataTypesToSet);
    }

    inline DataType getDataType(uint32_t idx) { return dataTypes[idx]; }

private:
    mutex orderBySharedStateMutex;

public:
    vector<shared_ptr<FactorizedTable>> factorizedTables;
    uint16_t nextFactorizedTableIdx;
    shared_ptr<queue<shared_ptr<MergedKeyBlocks>>> sortedKeyBlocks;

    uint64_t numBytesPerTuple;
    vector<StringAndUnstructuredKeyColInfo> stringAndUnstructuredKeyColInfo;
    vector<DataType> dataTypes;
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
        shared_ptr<SharedFactorizedTablesAndSortedKeyBlocks> sharedState,
        unique_ptr<PhysicalOperator> child, ExecutionContext& context, uint32_t id)
        : Sink{move(child), context, id}, factorizedTableIdx{0}, orderByDataInfo{orderByDataInfo},
          sharedState{move(sharedState)} {}

    shared_ptr<ResultSet> initResultSet() override;
    void execute() override;

    PhysicalOperatorType getOperatorType() override { return ORDER_BY; }

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<OrderBy>(
            orderByDataInfo, sharedState, children[0]->clone(), context, id);
    }

private:
    uint16_t factorizedTableIdx;
    OrderByDataInfo orderByDataInfo;
    unique_ptr<OrderByKeyEncoder> orderByKeyEncoder;
    unique_ptr<RadixSort> radixSorter;
    vector<shared_ptr<ValueVector>> keyVectors;
    vector<shared_ptr<ValueVector>> vectorsToAppend;
    vector<StringAndUnstructuredKeyColInfo> stringAndUnstructuredKeyColInfo;
    shared_ptr<SharedFactorizedTablesAndSortedKeyBlocks> sharedState;
    shared_ptr<FactorizedTable> localFactorizedTable;
};

} // namespace processor
} // namespace graphflow
