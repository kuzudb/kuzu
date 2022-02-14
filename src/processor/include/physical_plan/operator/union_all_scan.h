#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"
#include "src/processor/include/physical_plan/operator/result_collector.h"
#include "src/processor/include/physical_plan/operator/source_operator.h"
#include "src/processor/include/physical_plan/result/factorized_table.h"

namespace graphflow {
namespace processor {

class UnionAllScanMorsel {
public:
    UnionAllScanMorsel(uint32_t resultCollectorIdx, uint32_t tupleIdx, uint32_t numRows)
        : childIdx{resultCollectorIdx}, startTupleIdx{tupleIdx}, numTuples{numRows} {}

    inline uint32_t getChildIdx() { return childIdx; }
    inline uint32_t getStartTupleIdx() { return startTupleIdx; }
    inline uint32_t getNumTuples() { return numTuples; }

private:
    uint32_t childIdx;
    uint32_t startTupleIdx;
    uint32_t numTuples;
};

class UnionAllScanSharedState {
public:
    UnionAllScanSharedState(vector<unique_ptr<PhysicalOperator>>& unionAllChildren)
        : resultCollectorIdxToRead{0}, nextTupleIdxToRead{0}, unionAllChildren{unionAllChildren} {}

    inline shared_ptr<FactorizedTable> getFactorizedTable(uint32_t idx) {
        return ((ResultCollector*)(unionAllChildren[idx].get()))->getResultFactorizedTable();
    }

    // If there is an unflat column in the factorizedTable, we can only read one tuple at a
    // time.
    inline void initForScanning() {
        lock_guard<mutex> lck{unionAllScanSharedStateMutex};
        maxMorselSize = getFactorizedTable(0)->hasUnflatCol() ? 1 : DEFAULT_VECTOR_CAPACITY;
    }

    unique_ptr<UnionAllScanMorsel> getMorsel();

private:
    uint64_t maxMorselSize;
    uint64_t resultCollectorIdxToRead;
    uint64_t nextTupleIdxToRead;

    vector<unique_ptr<PhysicalOperator>>& unionAllChildren;
    mutex unionAllScanSharedStateMutex;
};

class UnionAllScan : public PhysicalOperator, public SourceOperator {
public:
    UnionAllScan(unique_ptr<ResultSetDescriptor> resultSetDescriptor, vector<DataPos> outDataPoses,
        vector<DataType> dataTypes, vector<unique_ptr<PhysicalOperator>> children,
        ExecutionContext& context, uint32_t id)
        : outDataPoses{outDataPoses}, dataTypes{dataTypes},
          PhysicalOperator{move(children), context, id}, SourceOperator{move(resultSetDescriptor)} {
        unionAllScanSharedState = make_shared<UnionAllScanSharedState>(this->children);
    }

    // For clone only.
    UnionAllScan(unique_ptr<ResultSetDescriptor> resultSetDescriptor, vector<DataPos> outDataPoses,
        vector<DataType> dataTypes, shared_ptr<UnionAllScanSharedState> unionAllScanSharedState,
        ExecutionContext& context, uint32_t id)
        : outDataPoses{outDataPoses}, dataTypes{dataTypes},
          unionAllScanSharedState{unionAllScanSharedState}, PhysicalOperator{context, id},
          SourceOperator{move(resultSetDescriptor)} {}

    shared_ptr<ResultSet> initResultSet() override;

    bool getNextTuples() override;

    PhysicalOperatorType getOperatorType() override { return UNION_ALL_SCAN; }

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<UnionAllScan>(resultSetDescriptor->copy(), outDataPoses, dataTypes,
            unionAllScanSharedState, context, id);
    }

private:
    shared_ptr<UnionAllScanSharedState> unionAllScanSharedState;
    vector<DataPos> outDataPoses;
    vector<DataType> dataTypes;
    vector<shared_ptr<ValueVector>> vectorsToRead;
};

} // namespace processor
} // namespace graphflow
