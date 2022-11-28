#pragma once

#include <queue>

#include "common/data_chunk/data_chunk_state.h"
#include "common/in_mem_overflow_buffer.h"
#include "processor/operator/order_by/radix_sort.h"
#include "processor/operator/sink.h"
#include "processor/result/factorized_table.h"
#include "processor/result/result_set.h"

using namespace std;
using namespace kuzu::common;

namespace kuzu {
namespace processor {

// This class contains factorizedTables, nextFactorizedTableIdx, strKeyColsInfo,
// sortedKeyBlocks and the size of each tuple in keyBlocks. The class is shared between the
// order_by, orderByMerge, orderByScan operators. All functions are guaranteed to be thread-safe, so
// caller doesn't need to acquire a lock before calling these functions.
class SharedFactorizedTablesAndSortedKeyBlocks {
public:
    explicit SharedFactorizedTablesAndSortedKeyBlocks()
        : nextFactorizedTableIdx{0}, sortedKeyBlocks{
                                         make_shared<queue<shared_ptr<MergedKeyBlocks>>>()} {}

    uint8_t getNextFactorizedTableIdx() {
        unique_lock lck{mtx};
        return nextFactorizedTableIdx++;
    }

    void appendFactorizedTable(
        uint8_t factorizedTableIdx, shared_ptr<FactorizedTable> factorizedTable) {
        unique_lock lck{mtx};
        // If the factorizedTables is full, resize the factorizedTables and
        // insert the factorizedTable to the set.
        if (factorizedTableIdx >= factorizedTables.size()) {
            factorizedTables.resize(factorizedTableIdx + 1);
        }
        factorizedTables[factorizedTableIdx] = move(factorizedTable);
    }

    void appendSortedKeyBlock(shared_ptr<MergedKeyBlocks> mergedDataBlocks) {
        unique_lock lck{mtx};
        sortedKeyBlocks->emplace(mergedDataBlocks);
    }

    void setNumBytesPerTuple(uint32_t _numBytesPerTuple) {
        assert(numBytesPerTuple == UINT32_MAX);
        numBytesPerTuple = _numBytesPerTuple;
    }

    void combineFTHasNoNullGuarantee() {
        for (auto i = 1u; i < factorizedTables.size(); i++) {
            factorizedTables[0]->mergeMayContainNulls(*factorizedTables[i]);
        }
    }

    void setStrKeyColInfo(vector<StrKeyColInfo> _strKeyColsInfo) {
        assert(strKeyColsInfo.empty());
        strKeyColsInfo = std::move(_strKeyColsInfo);
    }

private:
    mutex mtx;

public:
    vector<shared_ptr<FactorizedTable>> factorizedTables;
    uint8_t nextFactorizedTableIdx;
    shared_ptr<queue<shared_ptr<MergedKeyBlocks>>> sortedKeyBlocks;

    uint32_t numBytesPerTuple = UINT32_MAX; // encoding size
    vector<StrKeyColInfo> strKeyColsInfo;
};

struct OrderByDataInfo {
public:
    OrderByDataInfo(vector<pair<DataPos, DataType>> keysPosAndType,
        vector<pair<DataPos, DataType>> payloadsPosAndType, vector<bool> isPayloadFlat,
        vector<bool> isAscOrder, bool mayContainUnflatKey)
        : keysPosAndType{std::move(keysPosAndType)}, payloadsPosAndType{std::move(
                                                         payloadsPosAndType)},
          isPayloadFlat{std::move(isPayloadFlat)}, isAscOrder{std::move(isAscOrder)},
          mayContainUnflatKey{mayContainUnflatKey} {}

    OrderByDataInfo(const OrderByDataInfo& other)
        : OrderByDataInfo{other.keysPosAndType, other.payloadsPosAndType, other.isPayloadFlat,
              other.isAscOrder, other.mayContainUnflatKey} {}

public:
    vector<pair<DataPos, DataType>> keysPosAndType;
    vector<pair<DataPos, DataType>> payloadsPosAndType;
    vector<bool> isPayloadFlat;
    vector<bool> isAscOrder;
    // TODO(Ziyi): We should figure out unflat keys in a more general way.
    bool mayContainUnflatKey;
};

class OrderBy : public Sink {
public:
    OrderBy(const OrderByDataInfo& orderByDataInfo,
        shared_ptr<SharedFactorizedTablesAndSortedKeyBlocks> sharedState,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : Sink{std::move(child), id, paramsString}, orderByDataInfo{orderByDataInfo},
          sharedState{std::move(sharedState)} {}

    PhysicalOperatorType getOperatorType() override { return ORDER_BY; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;

    void finalize(ExecutionContext* context) override {
        // TODO(Ziyi): we always call lookup function on the first factorizedTable in sharedState
        // and that lookup function may read tuples in other factorizedTable, So we need to combine
        // hasNoNullGuarantee with other factorizedTables. This is not a good way to solve this
        // problem, and should be changed later.
        sharedState->combineFTHasNoNullGuarantee();
    }

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<OrderBy>(
            orderByDataInfo, sharedState, children[0]->clone(), id, paramsString);
    }

private:
    unique_ptr<FactorizedTableSchema> populateTableSchema();

    void initGlobalStateInternal(ExecutionContext* context) override;

private:
    uint8_t factorizedTableIdx;
    OrderByDataInfo orderByDataInfo;
    unique_ptr<OrderByKeyEncoder> orderByKeyEncoder;
    unique_ptr<RadixSort> radixSorter;
    vector<shared_ptr<ValueVector>> keyVectors;
    vector<shared_ptr<ValueVector>> vectorsToAppend;
    shared_ptr<SharedFactorizedTablesAndSortedKeyBlocks> sharedState;
    shared_ptr<FactorizedTable> localFactorizedTable;
};

} // namespace processor
} // namespace kuzu
