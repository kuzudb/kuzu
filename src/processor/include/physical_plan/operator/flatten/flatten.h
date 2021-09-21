#pragma once

#include "src/processor/include/physical_plan/operator/physical_operator.h"

namespace graphflow {
namespace processor {

class Flatten : public PhysicalOperator {

public:
    Flatten(uint32_t dataChunkToFlattenPos, unique_ptr<PhysicalOperator> prevOperator,
        ExecutionContext& context, uint32_t id)
        : PhysicalOperator{move(prevOperator), FLATTEN, context, id}, dataChunkToFlattenPos{
                                                                          dataChunkToFlattenPos} {}

    void initResultSet(const shared_ptr<ResultSet>& resultSet) override;

    bool getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<Flatten>(dataChunkToFlattenPos, prevOperator->clone(), context, id);
    }

private:
    uint32_t dataChunkToFlattenPos;
    shared_ptr<DataChunk> dataChunkToFlatten;
};

} // namespace processor
} // namespace graphflow
