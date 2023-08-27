#pragma once

#include <queue>

#include "function/binary_function_executor.h"
#include "processor/operator/sink.h"
#include "sort_state.h"

namespace kuzu {
namespace processor {

struct TopKScanState {
    // TODO(Xiyang): Move the initialization of payloadScanner to mapper.
    inline void init(MergedKeyBlocks* keyBlockToScan, std::vector<FactorizedTable*> payloadTables,
        uint64_t skipNum, uint64_t limitNum) {
        payloadScanner = std::make_unique<PayloadScanner>(
            keyBlockToScan, std::move(payloadTables), skipNum, limitNum);
    }

    std::unique_ptr<PayloadScanner> payloadScanner;
};

class TopKSortState {

public:
    TopKSortState();

    void init(const OrderByDataInfo& orderByDataInfo, storage::MemoryManager* memoryManager);

    void append(std::vector<common::ValueVector*> keyVectors,
        std::vector<common::ValueVector*> payloadVectors);

    void finalize();

    inline uint64_t getNumTuples() { return numTuples; }

    inline SortSharedState* getSharedState() { return orderBySharedState.get(); }

    inline void initScan(TopKScanState& scanState, uint64_t skip, uint64_t limit) {
        scanState.init(orderBySharedState->getMergedKeyBlock(),
            orderBySharedState->getPayloadTables(), skip, limit);
    }

private:
    std::unique_ptr<SortLocalState> orderByLocalState;
    std::unique_ptr<SortSharedState> orderBySharedState;

    uint64_t numTuples;
    storage::MemoryManager* memoryManager;
};

class TopKBuffer {
    using vector_select_comparison_func =
        std::function<bool(common::ValueVector&, common::ValueVector&, common::SelectionVector&)>;

public:
    TopKBuffer() { sortState = std::make_unique<TopKSortState>(); }

    void init(const OrderByDataInfo& orderByDataInfo, storage::MemoryManager* memoryManager,
        uint64_t skipNumber, uint64_t limitNumber);

    void append(std::vector<common::ValueVector*> keyVectors,
        std::vector<common::ValueVector*> payloadVectors);

    void reduce();

    inline void finalize() { sortState->finalize(); }

    void merge(TopKBuffer* other);

    inline void initScan(TopKScanState& scanState) { sortState->initScan(scanState, skip, limit); }

private:
    void initVectors();

    uint64_t findKeyVectorPosInPayload(const DataPos& keyPos);

    template<typename FUNC>
    void getSelectComparisonFunction(
        common::PhysicalTypeID typeID, vector_select_comparison_func& selectFunc);

    void initCompareFuncs();

    void setBoundaryValue();

    bool compareBoundaryValue(std::vector<common::ValueVector*>& keyVectors);

    bool compareFlatKeys(
        common::vector_idx_t vectorIdxToCompare, std::vector<common::ValueVector*> keyVectors);

    void compareUnflatKeys(
        common::vector_idx_t vectorIdxToCompare, std::vector<common::ValueVector*> keyVectors);

    static void appendSelState(
        common::SelectionVector* selVector, common::SelectionVector* selVectorToAppend);

public:
    std::unique_ptr<TopKSortState> sortState;
    uint64_t skip;
    uint64_t limit;
    const OrderByDataInfo* orderByDataInfo;
    storage::MemoryManager* memoryManager;
    std::vector<vector_select_comparison_func> compareFuncs;
    std::vector<vector_select_comparison_func> equalsFuncs;
    bool hasBoundaryValue = false;

private:
    // Holds the ownership of all temp vectors.
    std::vector<std::unique_ptr<common::ValueVector>> tmpVectors;
    std::vector<std::unique_ptr<common::ValueVector>> boundaryVecs;

    std::vector<common::ValueVector*> payloadVecsToScan;
    std::vector<common::ValueVector*> keyVecsToScan;
    std::vector<common::ValueVector*> lastPayloadVecsToScan;
    std::vector<common::ValueVector*> lastKeyVecsToScan;
};

class TopKLocalState {
public:
    TopKLocalState() { buffer = std::make_unique<TopKBuffer>(); }

    void init(const OrderByDataInfo& orderByDataInfo, storage::MemoryManager* memoryManager,
        ResultSet& resultSet, uint64_t skipNumber, uint64_t limitNumber);

    void append();

    inline void finalize() { buffer->finalize(); }

    std::unique_ptr<TopKBuffer> buffer;

private:
    std::vector<common::ValueVector*> orderByVectors;
    std::vector<common::ValueVector*> payloadVectors;
};

class TopKSharedState {
public:
    TopKSharedState() { buffer = std::make_unique<TopKBuffer>(); }

    void init(const OrderByDataInfo& orderByDataInfo, storage::MemoryManager* memoryManager,
        uint64_t skipNumber, uint64_t limitNumber) {
        buffer->init(orderByDataInfo, memoryManager, skipNumber, limitNumber);
    }

    void mergeLocalState(TopKLocalState* localState) {
        std::unique_lock lck{mtx};
        buffer->merge(localState->buffer.get());
    }

    void finalize() { buffer->finalize(); }

    std::unique_ptr<TopKBuffer> buffer;

private:
    std::mutex mtx;
};

class TopK : public Sink {
public:
    TopK(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::unique_ptr<TopKLocalState> localState, std::shared_ptr<TopKSharedState> sharedState,
        OrderByDataInfo orderByDataInfo, uint64_t skipNumber, uint64_t limitNumber,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : Sink{std::move(resultSetDescriptor), PhysicalOperatorType::TOP_K, std::move(child), id,
              paramsString},
          localState{std::move(localState)}, sharedState{std::move(sharedState)},
          orderByDataInfo{std::move(orderByDataInfo)}, skipNumber{skipNumber}, limitNumber{
                                                                                   limitNumber} {}

    inline void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final {
        localState->init(
            orderByDataInfo, context->memoryManager, *resultSet, skipNumber, limitNumber);
    }

    inline void initGlobalStateInternal(ExecutionContext* context) final {
        sharedState->init(orderByDataInfo, context->memoryManager, skipNumber, limitNumber);
    }

    void executeInternal(ExecutionContext* context) final;

    void finalize(ExecutionContext* context) final { sharedState->finalize(); }

    std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<TopK>(resultSetDescriptor->copy(),
            std::make_unique<TopKLocalState>(), sharedState, orderByDataInfo, skipNumber,
            limitNumber, children[0]->clone(), id, paramsString);
    }

private:
    std::unique_ptr<TopKLocalState> localState;
    std::shared_ptr<TopKSharedState> sharedState;
    OrderByDataInfo orderByDataInfo;
    uint64_t skipNumber;
    uint64_t limitNumber;
};

} // namespace processor
} // namespace kuzu
