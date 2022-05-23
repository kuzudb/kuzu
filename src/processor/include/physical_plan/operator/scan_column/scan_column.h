#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"

using namespace graphflow::storage;

namespace graphflow {
namespace processor {

class BaseScanColumn : public PhysicalOperator {

public:
    BaseScanColumn(const DataPos& inputNodeIDVectorPos, unique_ptr<PhysicalOperator> child,
        uint32_t id, const string& paramsString)
        : PhysicalOperator{move(child), id, paramsString}, inputNodeIDVectorPos{
                                                               inputNodeIDVectorPos} {}

    PhysicalOperatorType getOperatorType() override = 0;

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    inline void reInitToRerunSubPlan() override { children[0]->reInitToRerunSubPlan(); }

protected:
    DataPos inputNodeIDVectorPos;

    shared_ptr<DataChunk> inputNodeIDDataChunk;
    shared_ptr<ValueVector> inputNodeIDVector;
};

class ScanSingleColumn : public BaseScanColumn {

protected:
    ScanSingleColumn(const DataPos& inputNodeIDVectorPos, const DataPos& outputVectorPos,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : BaseScanColumn{inputNodeIDVectorPos, move(child), id, paramsString},
          outputVectorPos{outputVectorPos} {}

protected:
    DataPos outputVectorPos;

    shared_ptr<ValueVector> outputVector;
};

class ScanMultipleColumns : public BaseScanColumn {

protected:
    ScanMultipleColumns(const DataPos& inputNodeIDVectorPos, vector<DataPos> outputVectorsPos,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : BaseScanColumn{inputNodeIDVectorPos, move(child), id, paramsString},
          outputVectorsPos{move(outputVectorsPos)} {}

protected:
    vector<DataPos> outputVectorsPos;
    vector<shared_ptr<ValueVector>> outputVectors;
};

} // namespace processor
} // namespace graphflow
