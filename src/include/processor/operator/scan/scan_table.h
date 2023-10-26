#pragma once

#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class ScanTable : public PhysicalOperator {
public:
    ScanTable(PhysicalOperatorType operatorType, const DataPos& inVectorPos,
        std::vector<DataPos> outVectorsPos, std::unique_ptr<PhysicalOperator> child, uint32_t id,
        const std::string& paramString)
        : PhysicalOperator{operatorType, std::move(child), id, paramString},
          inVectorPos{inVectorPos}, outVectorsPos{std::move(outVectorsPos)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* executionContext);

protected:
    DataPos inVectorPos;
    std::vector<DataPos> outVectorsPos;
    common::ValueVector* inVector;
    std::vector<common::ValueVector*> outVectors;
};

} // namespace processor
} // namespace kuzu
