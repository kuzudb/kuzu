#pragma once

#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class Flatten : public PhysicalOperator {

public:
    Flatten(uint32_t dataChunkToFlattenPos, unique_ptr<PhysicalOperator> child, uint32_t id,
        const string& paramsString)
        : PhysicalOperator{move(child), id, paramsString}, dataChunkToFlattenPos{
                                                               dataChunkToFlattenPos} {}

    PhysicalOperatorType getOperatorType() override { return FLATTEN; }

    shared_ptr<ResultSet> init(ExecutionContext* context) override;

    bool getNextTuplesInternal() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<Flatten>(dataChunkToFlattenPos, children[0]->clone(), id, paramsString);
    }

private:
    uint32_t dataChunkToFlattenPos;
    shared_ptr<DataChunk> dataChunkToFlatten;
};

} // namespace processor
} // namespace kuzu
