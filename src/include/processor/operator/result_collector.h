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
        MemoryManager* memoryManager, unique_ptr<FactorizedTableSchema> tableSchema);

    inline void mergeLocalTable(FactorizedTable& localTable) {
        lock_guard<mutex> lck{mtx};
        table->merge(localTable);
    }

    inline shared_ptr<FactorizedTable> getTable() { return table; }

    inline uint64_t getMaxMorselSize() {
        lock_guard<mutex> lck{mtx};
        return table->hasUnflatCol() ? 1 : DEFAULT_VECTOR_CAPACITY;
    }
    unique_ptr<FTableScanMorsel> getMorsel(uint64_t maxMorselSize);

private:
    mutex mtx;
    shared_ptr<FactorizedTable> table;

    uint64_t nextTupleIdxToScan = 0u;
};

class ResultCollector : public Sink {
public:
    ResultCollector(vector<pair<DataPos, DataType>> payloadsPosAndType, vector<bool> isPayloadFlat,
        shared_ptr<FTableSharedState> sharedState, unique_ptr<PhysicalOperator> child, uint32_t id,
        const string& paramsString)
        : Sink{std::move(child), id, paramsString}, payloadsPosAndType{std::move(
                                                        payloadsPosAndType)},
          isPayloadFlat{std::move(isPayloadFlat)}, sharedState{std::move(sharedState)} {}

    PhysicalOperatorType getOperatorType() override { return RESULT_COLLECTOR; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    void executeInternal(ExecutionContext* context) override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ResultCollector>(
            payloadsPosAndType, isPayloadFlat, sharedState, children[0]->clone(), id, paramsString);
    }

    inline shared_ptr<FTableSharedState> getSharedState() { return sharedState; }
    inline shared_ptr<FactorizedTable> getResultFactorizedTable() {
        return sharedState->getTable();
    }

private:
    void initGlobalStateInternal(ExecutionContext* context) override;

    unique_ptr<FactorizedTableSchema> populateTableSchema();

private:
    vector<pair<DataPos, DataType>> payloadsPosAndType;
    vector<bool> isPayloadFlat;
    vector<shared_ptr<ValueVector>> vectorsToCollect;
    shared_ptr<FTableSharedState> sharedState;
    unique_ptr<FactorizedTable> localTable;
};

} // namespace processor
} // namespace kuzu
