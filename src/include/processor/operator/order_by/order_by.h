#pragma once

#include <queue>

#include "common/data_chunk/data_chunk_state.h"
#include "common/in_mem_overflow_buffer.h"
#include "processor/operator/order_by/radix_sort.h"
#include "processor/operator/sink.h"
#include "processor/result/factorized_table.h"
#include "processor/result/result_set.h"

namespace kuzu {
namespace processor {

// This class contains factorizedTables, nextFactorizedTableIdx, strKeyColsInfo,
// sortedKeyBlocks and the size of each tuple in keyBlocks. The class is shared between the
// order_by, orderByMerge, orderByScan operators. All functions are guaranteed to be thread-safe, so
// caller doesn't need to acquire a lock before calling these functions.
class SharedFactorizedTablesAndSortedKeyBlocks {
public:
    explicit SharedFactorizedTablesAndSortedKeyBlocks()
        : nextFactorizedTableIdx{0},
          sortedKeyBlocks{std::make_shared<std::queue<std::shared_ptr<MergedKeyBlocks>>>()} {}

    uint8_t getNextFactorizedTableIdx() {
        std::unique_lock lck{mtx};
        return nextFactorizedTableIdx++;
    }

    void appendFactorizedTable(
        uint8_t factorizedTableIdx, std::shared_ptr<FactorizedTable> factorizedTable) {
        std::unique_lock lck{mtx};
        // If the factorizedTables is full, resize the factorizedTables and
        // insert the factorizedTable to the set.
        if (factorizedTableIdx >= factorizedTables.size()) {
            factorizedTables.resize(factorizedTableIdx + 1);
        }
        factorizedTables[factorizedTableIdx] = std::move(factorizedTable);
    }

    void appendSortedKeyBlock(std::shared_ptr<MergedKeyBlocks> mergedDataBlocks) {
        std::unique_lock lck{mtx};
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

    void setStrKeyColInfo(std::vector<StrKeyColInfo> _strKeyColsInfo) {
        assert(strKeyColsInfo.empty());
        strKeyColsInfo = std::move(_strKeyColsInfo);
    }

private:
    std::mutex mtx;

public:
    std::vector<std::shared_ptr<FactorizedTable>> factorizedTables;
    uint8_t nextFactorizedTableIdx;
    std::shared_ptr<std::queue<std::shared_ptr<MergedKeyBlocks>>> sortedKeyBlocks;

    uint32_t numBytesPerTuple = UINT32_MAX; // encoding size
    std::vector<StrKeyColInfo> strKeyColsInfo;
};

struct OrderByDataInfo {
public:
    OrderByDataInfo(std::vector<std::pair<DataPos, common::LogicalType>> keysPosAndType,
        std::vector<std::pair<DataPos, common::LogicalType>> payloadsPosAndType,
        std::vector<bool> isPayloadFlat, std::vector<bool> isAscOrder, bool mayContainUnflatKey)
        : keysPosAndType{std::move(keysPosAndType)}, payloadsPosAndType{std::move(
                                                         payloadsPosAndType)},
          isPayloadFlat{std::move(isPayloadFlat)}, isAscOrder{std::move(isAscOrder)},
          mayContainUnflatKey{mayContainUnflatKey} {}

    OrderByDataInfo(const OrderByDataInfo& other)
        : OrderByDataInfo{other.keysPosAndType, other.payloadsPosAndType, other.isPayloadFlat,
              other.isAscOrder, other.mayContainUnflatKey} {}

public:
    std::vector<std::pair<DataPos, common::LogicalType>> keysPosAndType;
    std::vector<std::pair<DataPos, common::LogicalType>> payloadsPosAndType;
    std::vector<bool> isPayloadFlat;
    std::vector<bool> isAscOrder;
    // TODO(Ziyi): We should figure out unflat keys in a more general way.
    bool mayContainUnflatKey;
};

class OrderBy : public Sink {
public:
    OrderBy(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        const OrderByDataInfo& orderByDataInfo,
        std::shared_ptr<SharedFactorizedTablesAndSortedKeyBlocks> sharedState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : Sink{std::move(resultSetDescriptor), PhysicalOperatorType::ORDER_BY, std::move(child), id,
              paramsString},
          orderByDataInfo{orderByDataInfo}, sharedState{std::move(sharedState)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;

    void finalize(ExecutionContext* context) override {
        // TODO(Ziyi): we always call lookup function on the first factorizedTable in sharedState
        // and that lookup function may read tuples in other factorizedTable, So we need to combine
        // hasNoNullGuarantee with other factorizedTables. This is not a good way to solve this
        // problem, and should be changed later.
        sharedState->combineFTHasNoNullGuarantee();
    }

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<OrderBy>(resultSetDescriptor->copy(), orderByDataInfo, sharedState,
            children[0]->clone(), id, paramsString);
    }

private:
    std::unique_ptr<FactorizedTableSchema> populateTableSchema();

    void initGlobalStateInternal(ExecutionContext* context) override;

private:
    uint8_t factorizedTableIdx;
    OrderByDataInfo orderByDataInfo;
    std::unique_ptr<OrderByKeyEncoder> orderByKeyEncoder;
    std::unique_ptr<RadixSort> radixSorter;
    std::vector<common::ValueVector*> keyVectors;
    std::vector<common::ValueVector*> vectorsToAppend;
    std::shared_ptr<SharedFactorizedTablesAndSortedKeyBlocks> sharedState;
    std::shared_ptr<FactorizedTable> localFactorizedTable;
};

} // namespace processor
} // namespace kuzu
