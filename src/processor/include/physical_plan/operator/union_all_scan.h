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

    inline uint32_t getChildIdx() const { return childIdx; }
    inline uint32_t getStartTupleIdx() const { return startTupleIdx; }
    inline uint32_t getNumTuples() const { return numTuples; }

private:
    uint32_t childIdx;
    uint32_t startTupleIdx;
    uint32_t numTuples;
};

class UnionAllScanSharedState {
public:
    explicit UnionAllScanSharedState(vector<unique_ptr<PhysicalOperator>>& unionAllChildren)
        : maxMorselSize{0}, resultCollectorIdxToRead{0}, nextTupleIdxToRead{0},
          unionAllChildren{unionAllChildren} {}

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
        vector<DataType> dataTypes, vector<unique_ptr<PhysicalOperator>> children, uint32_t id)
        : PhysicalOperator{move(children), id}, SourceOperator{move(resultSetDescriptor)},
          outDataPoses{move(outDataPoses)}, dataTypes{move(dataTypes)} {
        unionAllScanSharedState = make_shared<UnionAllScanSharedState>(this->children);
    }

    // For clone only.
    UnionAllScan(unique_ptr<ResultSetDescriptor> resultSetDescriptor, vector<DataPos> outDataPoses,
        vector<DataType> dataTypes, shared_ptr<UnionAllScanSharedState> unionAllScanSharedState,
        uint32_t id)
        : PhysicalOperator{id}, SourceOperator{move(resultSetDescriptor)},
          unionAllScanSharedState{move(unionAllScanSharedState)},
          outDataPoses{move(outDataPoses)}, dataTypes{move(dataTypes)} {}

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    bool getNextTuples() override;

    PhysicalOperatorType getOperatorType() override { return UNION_ALL_SCAN; }

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<UnionAllScan>(
            resultSetDescriptor->copy(), outDataPoses, dataTypes, unionAllScanSharedState, id);
    }

private:
    shared_ptr<UnionAllScanSharedState> unionAllScanSharedState;
    vector<DataPos> outDataPoses;
    vector<DataType> dataTypes;
    vector<shared_ptr<ValueVector>> vectorsToRead;
};

} // namespace processor
} // namespace graphflow
