#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"

namespace graphflow {
namespace processor {

/**
 * SelectScan is the scan (first) operator for subquery. It is responsible to copy a flat tuple from
 * outer query into inner query and controls when to terminate inner query.
 */
class SelectScan : public PhysicalOperator {

public:
    SelectScan(const ResultSet* inResultSet, vector<pair<uint64_t, uint64_t>> valueVectorsPosToSelect, ExecutionContext& context,
        uint32_t id);

    inline void setInResultSet(const ResultSet* resultSet) { inResultSet = resultSet; }

    void reInitialize() override;

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<SelectScan>(inResultSet, valueVectorsPosToSelect, context, id);
    }

private:
    const ResultSet* inResultSet;
    vector<pair<uint64_t, uint64_t>> valueVectorsPosToSelect;

    bool isFirstExecution;
    shared_ptr<DataChunk> outDataChunk;
};

} // namespace processor
} // namespace graphflow