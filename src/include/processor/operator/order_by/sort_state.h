#pragma once

#include <queue>

#include "processor/operator/order_by/radix_sort.h"
#include "processor/result/factorized_table.h"

namespace kuzu {
namespace processor {

struct LocalPayloadTableInfo {
    uint64_t globalIdx;
    FactorizedTable* payloadTable;
};

class SortSharedState {
public:
    SortSharedState()
        : nextFactorizedTableIdx{0},
          sortedKeyBlocks{std::make_shared<std::queue<std::shared_ptr<MergedKeyBlocks>>>()} {};

    inline uint64_t getNumBytesPerTuple() const { return numBytesPerTuple; }

    inline std::vector<StrKeyColInfo>& getStrKeyColInfo() { return strKeyColsInfo; }

    inline std::queue<std::shared_ptr<MergedKeyBlocks>>* getSortedKeyBlocks() {
        return sortedKeyBlocks.get();
    }

    void init(const OrderByDataInfo& orderByDataInfo);

    LocalPayloadTableInfo getLocalPayloadTable(storage::MemoryManager& memoryManager);

    void appendLocalSortedKeyBlock(std::shared_ptr<MergedKeyBlocks> mergedDataBlocks);

    void combineFTHasNoNullGuarantee();

    std::vector<FactorizedTable*> getPayloadTables() const;

    inline MergedKeyBlocks* getMergedKeyBlock() const {
        return sortedKeyBlocks->empty() ? nullptr : sortedKeyBlocks->front().get();
    }

private:
    void calculatePayloadSchema(const kuzu::processor::OrderByDataInfo& orderByDataInfo);

private:
    std::mutex mtx;
    std::vector<std::unique_ptr<FactorizedTable>> payloadTables;
    uint8_t nextFactorizedTableIdx;
    std::shared_ptr<std::queue<std::shared_ptr<MergedKeyBlocks>>> sortedKeyBlocks;
    uint32_t numBytesPerTuple;
    std::vector<StrKeyColInfo> strKeyColsInfo;

private:
    std::unique_ptr<FactorizedTableSchema> payloadSchema;
};

class SortLocalState {
public:
    void init(const OrderByDataInfo& orderByDataInfo, SortSharedState& sharedState,
        storage::MemoryManager* memoryManager);

    void append(std::vector<common::ValueVector*> keyVectors,
        std::vector<common::ValueVector*> payloadVectors);

    void finalize(SortSharedState& sharedState);

private:
    std::unique_ptr<OrderByKeyEncoder> orderByKeyEncoder;
    std::unique_ptr<RadixSort> radixSorter;
    LocalPayloadTableInfo localPayloadTableInfo;
};

class PayloadScanner {
public:
    PayloadScanner(MergedKeyBlocks* keyBlockToScan, std::vector<FactorizedTable*> payloadTables,
        uint64_t skipNumber = UINT64_MAX, uint64_t limitNumber = UINT64_MAX);

    uint64_t scan(std::vector<common::ValueVector*> vectorsToRead);

private:
    bool scanSingleTuple;
    uint32_t payloadIdxOffset;
    std::vector<uint32_t> colsToScan;
    std::unique_ptr<uint8_t*[]> tuplesToRead;
    std::unique_ptr<BlockPtrInfo> blockPtrInfo;
    MergedKeyBlocks* keyBlockToScan;
    uint32_t nextTupleIdxToReadInMergedKeyBlock;
    uint64_t endTuplesIdxToReadInMergedKeyBlock;
    std::vector<FactorizedTable*> payloadTables;
};

} // namespace processor
} // namespace kuzu
