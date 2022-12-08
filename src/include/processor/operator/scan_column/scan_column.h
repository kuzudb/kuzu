#pragma once

#include "processor/operator/physical_operator.h"

using namespace kuzu::storage;

namespace kuzu {
namespace processor {

class BaseScanColumn : public PhysicalOperator {
public:
    BaseScanColumn(const DataPos& inputNodeIDVectorPos, unique_ptr<PhysicalOperator> child,
        uint32_t id, const string& paramsString)
        : PhysicalOperator{move(child), id, paramsString}, inputNodeIDVectorPos{
                                                               inputNodeIDVectorPos} {}

    PhysicalOperatorType getOperatorType() override = 0;

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

protected:
    DataPos inputNodeIDVectorPos;

    shared_ptr<ValueVector> inputNodeIDVector;
};

class ScanSingleColumn : public BaseScanColumn {
protected:
    ScanSingleColumn(const DataPos& inputNodeIDVectorPos, const DataPos& outputVectorPos,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : BaseScanColumn{inputNodeIDVectorPos, move(child), id, paramsString},
          outputVectorPos{outputVectorPos} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

protected:
    DataPos outputVectorPos;
    shared_ptr<ValueVector> outputVector;
};

class ScanMultipleColumns : public BaseScanColumn {
protected:
    ScanMultipleColumns(const DataPos& inVectorPos, vector<DataPos> outVectorsPos,
        unique_ptr<PhysicalOperator> child, uint32_t id, const string& paramsString)
        : BaseScanColumn{inVectorPos, std::move(child), id, paramsString}, outVectorsPos{std::move(
                                                                               outVectorsPos)} {}

    inline PhysicalOperatorType getOperatorType() override {
        return PhysicalOperatorType::SCAN_COLUMN_PROPERTY;
    }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

protected:
    vector<DataPos> outVectorsPos;
    vector<shared_ptr<ValueVector>> outVectors;
};

} // namespace processor
} // namespace kuzu
