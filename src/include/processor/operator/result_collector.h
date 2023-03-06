#pragma once

#include "processor/operator/sink.h"
#include "processor/result/factorized_table.h"

namespace kuzu {
namespace processor {

struct FTableScanMorsel {

    FTableScanMorsel(FactorizedTable* table, uint64_t startTupleIdx, uint64_t numTuples)
        : table{table}, startTupleIdx{startTupleIdx}, numTuples{numTuples} {}

    FactorizedTable* table;
    uint64_t startTupleIdx;
    uint64_t numTuples;
};

class FTableSharedState {
public:
    void initTableIfNecessary(
        storage::MemoryManager* memoryManager, std::unique_ptr<FactorizedTableSchema> tableSchema);

    inline void mergeLocalTable(FactorizedTable& localTable) {
        std::lock_guard<std::mutex> lck{mtx};
        table->merge(localTable);
    }

    inline std::shared_ptr<FactorizedTable> getTable() { return table; }

    inline uint64_t getMaxMorselSize() {
        std::lock_guard<std::mutex> lck{mtx};
        return table->hasUnflatCol() ? 1 : common::DEFAULT_VECTOR_CAPACITY;
    }
    std::unique_ptr<FTableScanMorsel> getMorsel(uint64_t maxMorselSize);

private:
    std::mutex mtx;
    std::shared_ptr<FactorizedTable> table;

    uint64_t nextTupleIdxToScan = 0u;
};

class ResultCollector : public Sink {
public:
    ResultCollector(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::vector<std::pair<DataPos, common::DataType>> payloadsPosAndType,
        std::vector<bool> isPayloadFlat, std::shared_ptr<FTableSharedState> sharedState,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : Sink{std::move(resultSetDescriptor), PhysicalOperatorType::RESULT_COLLECTOR,
              std::move(child), id, paramsString},
          payloadsPosAndType{std::move(payloadsPosAndType)},
          isPayloadFlat{std::move(isPayloadFlat)}, sharedState{std::move(sharedState)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ResultCollector>(resultSetDescriptor->copy(), payloadsPosAndType,
            isPayloadFlat, sharedState, children[0]->clone(), id, paramsString);
    }

    inline std::shared_ptr<FTableSharedState> getSharedState() { return sharedState; }
    inline std::shared_ptr<FactorizedTable> getResultFactorizedTable() {
        return sharedState->getTable();
    }

private:
    void initGlobalStateInternal(ExecutionContext* context) override;

    std::unique_ptr<FactorizedTableSchema> populateTableSchema();

private:
    std::vector<std::pair<DataPos, common::DataType>> payloadsPosAndType;
    std::vector<bool> isPayloadFlat;
    std::vector<common::ValueVector*> vectorsToCollect;
    std::shared_ptr<FTableSharedState> sharedState;
    std::unique_ptr<FactorizedTable> localTable;
};

} // namespace processor
} // namespace kuzu
