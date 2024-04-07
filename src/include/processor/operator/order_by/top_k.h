#pragma once

#include "processor/operator/sink.h"
#include "sort_state.h"

namespace kuzu {
namespace processor {

class TopKSortState {
public:
    TopKSortState();

    void init(const OrderByDataInfo& orderByDataInfo, storage::MemoryManager* memoryManager);

    void append(const std::vector<common::ValueVector*>& keyVectors,
        const std::vector<common::ValueVector*>& payloadVectors);

    void finalize();

    inline uint64_t getNumTuples() const { return numTuples; }

    inline SortSharedState* getSharedState() { return orderBySharedState.get(); }

    std::unique_ptr<PayloadScanner> getScanner(uint64_t skip, uint64_t limit) const {
        return std::make_unique<PayloadScanner>(orderBySharedState->getMergedKeyBlock(),
            orderBySharedState->getPayloadTables(), skip, limit);
    }

private:
    std::unique_ptr<SortLocalState> orderByLocalState;
    std::unique_ptr<SortSharedState> orderBySharedState;

    uint64_t numTuples = 0;
    storage::MemoryManager* memoryManager = nullptr;
};

class TopKBuffer {
    using vector_select_comparison_func =
        std::function<bool(common::ValueVector&, common::ValueVector&, common::SelectionVector&)>;

public:
    explicit TopKBuffer(const OrderByDataInfo& orderByDataInfo)
        : orderByDataInfo{&orderByDataInfo} {
        sortState = std::make_unique<TopKSortState>();
    }

    void init(storage::MemoryManager* memoryManager, uint64_t skipNumber, uint64_t limitNumber);

    void append(const std::vector<common::ValueVector*>& keyVectors,
        const std::vector<common::ValueVector*>& payloadVectors);

    void reduce();

    // NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const.
    inline void finalize() { sortState->finalize(); }

    void merge(TopKBuffer* other);

    inline std::unique_ptr<PayloadScanner> getScanner() const {
        return sortState->getScanner(skip, limit);
    }

private:
    void initVectors();

    template<typename FUNC>
    void getSelectComparisonFunction(common::PhysicalTypeID typeID,
        vector_select_comparison_func& selectFunc);

    void initCompareFuncs();

    void setBoundaryValue();

    bool compareBoundaryValue(const std::vector<common::ValueVector*>& keyVectors);

    bool compareFlatKeys(common::vector_idx_t vectorIdxToCompare,
        const std::vector<common::ValueVector*> keyVectors);

    void compareUnflatKeys(common::vector_idx_t vectorIdxToCompare,
        const std::vector<common::ValueVector*> keyVectors);

    static void appendSelState(common::SelectionVector* selVector,
        common::SelectionVector* selVectorToAppend);

public:
    const OrderByDataInfo* orderByDataInfo;
    std::unique_ptr<TopKSortState> sortState;
    uint64_t skip;
    uint64_t limit;
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
    void init(const OrderByDataInfo& orderByDataInfo, storage::MemoryManager* memoryManager,
        ResultSet& resultSet, uint64_t skipNumber, uint64_t limitNumber);

    void append(const std::vector<common::ValueVector*>& keyVectors,
        const std::vector<common::ValueVector*>& payloadVectors);

    // NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const.
    inline void finalize() { buffer->finalize(); }

    std::unique_ptr<TopKBuffer> buffer;
};

class TopKSharedState {
public:
    void init(const OrderByDataInfo& orderByDataInfo, storage::MemoryManager* memoryManager,
        uint64_t skipNumber, uint64_t limitNumber) {
        buffer = std::make_unique<TopKBuffer>(orderByDataInfo);
        buffer->init(memoryManager, skipNumber, limitNumber);
    }

    void mergeLocalState(TopKLocalState* localState) {
        std::unique_lock lck{mtx};
        buffer->merge(localState->buffer.get());
    }

    // NOLINTNEXTLINE(readability-make-member-function-const): Semantically non-const.
    inline void finalize() { buffer->finalize(); }

    std::unique_ptr<TopKBuffer> buffer;

private:
    std::mutex mtx;
};

class TopK final : public Sink {
public:
    TopK(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::unique_ptr<OrderByDataInfo> info, std::shared_ptr<TopKSharedState> sharedState,
        uint64_t skipNumber, uint64_t limitNumber, std::unique_ptr<PhysicalOperator> child,
        uint32_t id, const std::string& paramsString)
        : Sink{std::move(resultSetDescriptor), PhysicalOperatorType::TOP_K, std::move(child), id,
              paramsString},
          info(std::move(info)), sharedState{std::move(sharedState)}, skipNumber{skipNumber},
          limitNumber{limitNumber} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    inline void initGlobalStateInternal(ExecutionContext* context) override {
        sharedState->init(*info, context->clientContext->getMemoryManager(), skipNumber,
            limitNumber);
    }

    void executeInternal(ExecutionContext* context) override;

    void finalize(ExecutionContext* /*context*/) override { sharedState->finalize(); }

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<TopK>(resultSetDescriptor->copy(), info->copy(), sharedState,
            skipNumber, limitNumber, children[0]->clone(), id, paramsString);
    }

private:
    std::unique_ptr<OrderByDataInfo> info;
    std::unique_ptr<TopKLocalState> localState;
    std::shared_ptr<TopKSharedState> sharedState;
    uint64_t skipNumber;
    uint64_t limitNumber;
    std::vector<common::ValueVector*> orderByVectors;
    std::vector<common::ValueVector*> payloadVectors;
};

} // namespace processor
} // namespace kuzu
