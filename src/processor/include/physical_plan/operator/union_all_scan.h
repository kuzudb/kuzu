#pragma once

#include "src/processor/include/physical_plan/operator/base_table_scan.h"

namespace graphflow {
namespace processor {

class UnionAllScanSharedState {

public:
    explicit UnionAllScanSharedState(vector<shared_ptr<FTableSharedState>> fTableSharedStates)
        : fTableSharedStates{move(fTableSharedStates)}, fTableToScanIdx{0} {}

    uint64_t getMaxMorselSize() const;
    unique_ptr<FTableScanMorsel> getMorsel(uint64_t maxMorselSize);

private:
    mutex mtx;
    vector<shared_ptr<FTableSharedState>> fTableSharedStates;
    uint64_t fTableToScanIdx;
};

class UnionAllScan : public BaseTableScan {

public:
    UnionAllScan(unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        vector<DataPos> outVecPositions, vector<DataType> outVecDataTypes,
        shared_ptr<UnionAllScanSharedState> sharedState,
        vector<unique_ptr<PhysicalOperator>> children, uint32_t id, const string& paramsString)
        : BaseTableScan{move(resultSetDescriptor), move(outVecPositions), move(outVecDataTypes),
              move(children), id, paramsString},
          sharedState{move(sharedState)} {}

    // For clone only
    UnionAllScan(unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        vector<DataPos> outVecPositions, vector<DataType> outVecDataTypes,
        shared_ptr<UnionAllScanSharedState> sharedState, uint32_t id, const string& paramsString)
        : BaseTableScan{move(resultSetDescriptor), move(outVecPositions), move(outVecDataTypes), id,
              paramsString},
          sharedState{move(sharedState)} {}

    inline void setMaxMorselSize() override { maxMorselSize = sharedState->getMaxMorselSize(); }
    inline unique_ptr<FTableScanMorsel> getMorsel() override {
        return sharedState->getMorsel(maxMorselSize);
    }

    PhysicalOperatorType getOperatorType() override { return UNION_ALL_SCAN; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override {
        PhysicalOperator::init(context);
        resultSet = populateResultSet();
        initFurther(context);
        return resultSet;
    }

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<UnionAllScan>(resultSetDescriptor->copy(), outVecPositions,
            outVecDataTypes, sharedState, id, paramsString);
    }

private:
    shared_ptr<UnionAllScanSharedState> sharedState;
};

} // namespace processor
} // namespace graphflow
