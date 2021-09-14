#pragma once

#include "src/processor/include/physical_plan/operator/scan_attribute/scan_attribute.h"
#include "src/storage/include/data_structure/column.h"

using namespace graphflow::storage;

namespace graphflow {
namespace processor {

class ScanColumn : public ScanAttribute {

public:
    ScanColumn(const DataPos& inDataPos, const DataPos& outDataPos, Column* column,
        unique_ptr<PhysicalOperator> prevOperator, ExecutionContext& context, uint32_t id)
        : ScanAttribute{inDataPos, outDataPos, move(prevOperator), context, id}, column{column} {};

    ~ScanColumn() override{};

    void initResultSet(const shared_ptr<ResultSet>& resultSet) override {
        ScanAttribute::initResultSet(resultSet);
    }

    void getNextTuples() override;

protected:
    Column* column;
};

} // namespace processor
} // namespace graphflow
