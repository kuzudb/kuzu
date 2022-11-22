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

    inline uint64_t getMaxMorselSize() const {
        return table->hasUnflatCol() ? 1 : DEFAULT_VECTOR_CAPACITY;
    }
    unique_ptr<FTableScanMorsel> getMorsel(uint64_t maxMorselSize);

    // A factorized table might be scanned multiple times.
    inline void setToInitialState() {
        lock_guard<mutex> lck{mtx};
        nextTupleIdxToScan = 0u;
    }

private:
    mutex mtx;
    shared_ptr<FactorizedTable> table;

    uint64_t nextTupleIdxToScan = 0u;
};

class ResultCollector : public Sink {

public:
    ResultCollector(vector<pair<DataPos, bool>> vectorsToCollectInfo,
        shared_ptr<FTableSharedState> sharedState, unique_ptr<PhysicalOperator> child, uint32_t id,
        const string& paramsString)
        : Sink{move(child), id, paramsString}, vectorsToCollectInfo{move(vectorsToCollectInfo)},
          sharedState{move(sharedState)} {}

    PhysicalOperatorType getOperatorType() override { return RESULT_COLLECTOR; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    void execute(ExecutionContext* context) override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ResultCollector>(
            vectorsToCollectInfo, sharedState, children[0]->clone(), id, paramsString);
    }

    inline shared_ptr<FTableSharedState> getSharedState() { return sharedState; }
    inline shared_ptr<FactorizedTable> getResultFactorizedTable() {
        return sharedState->getTable();
    }

private:
    vector<pair<DataPos, bool>> vectorsToCollectInfo;
    vector<shared_ptr<ValueVector>> vectorsToCollect;
    shared_ptr<FTableSharedState> sharedState;
    unique_ptr<FactorizedTable> localTable;
};

} // namespace processor
} // namespace kuzu
