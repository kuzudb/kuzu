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
    FTableSharedState(std::shared_ptr<FactorizedTable> table, uint64_t maxMorselSize)
        : table{std::move(table)}, maxMorselSize{maxMorselSize} {}
    FTableSharedState(storage::MemoryManager* memoryManager,
        std::unique_ptr<FactorizedTableSchema> tableSchema, uint64_t maxMorselSize)
        : maxMorselSize{maxMorselSize}, nextTupleIdxToScan{0} {
        table = std::make_shared<FactorizedTable>(memoryManager, std::move(tableSchema));
    }

    // We want to control the granularity of morsel, e.g. in recursive join pipeline, we always want
    // to scan 1 src at a time.
    inline void setMaxMorselSize(uint64_t size) { maxMorselSize = size; }
    inline uint64_t getMaxMorselSize() const { return maxMorselSize; }

    inline void mergeLocalTable(FactorizedTable& localTable) {
        std::lock_guard<std::mutex> lck{mtx};
        table->merge(localTable);
    }

    inline std::shared_ptr<FactorizedTable> getTable() { return table; }

    std::unique_ptr<FTableScanMorsel> getMorsel();

private:
    std::mutex mtx;
    std::shared_ptr<FactorizedTable> table;
    uint64_t maxMorselSize;

    uint64_t nextTupleIdxToScan = 0u;
};

class ResultCollector : public Sink {
public:
    ResultCollector(std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::unique_ptr<FactorizedTableSchema> tableSchema, std::vector<DataPos> payloadsPos,
        std::shared_ptr<FTableSharedState> sharedState, std::unique_ptr<PhysicalOperator> child,
        uint32_t id, const std::string& paramsString)
        : Sink{std::move(resultSetDescriptor), PhysicalOperatorType::RESULT_COLLECTOR,
              std::move(child), id, paramsString},
          tableSchema{std::move(tableSchema)}, payloadsPos{std::move(payloadsPos)},
          sharedState{std::move(sharedState)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;

    inline std::shared_ptr<FTableSharedState> getSharedState() { return sharedState; }
    inline std::shared_ptr<FactorizedTable> getResultFactorizedTable() {
        return sharedState->getTable();
    }

    std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ResultCollector>(resultSetDescriptor->copy(), tableSchema->copy(),
            payloadsPos, sharedState, children[0]->clone(), id, paramsString);
    }

private:
    std::unique_ptr<FactorizedTableSchema> tableSchema;
    std::vector<DataPos> payloadsPos;
    std::vector<common::ValueVector*> payloadVectors;
    std::shared_ptr<FTableSharedState> sharedState;
    std::unique_ptr<FactorizedTable> localTable;
};

} // namespace processor
} // namespace kuzu
