#pragma once

#include "src/common/include/vector/node_vector.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"

namespace graphflow {
namespace processor {

class ReadList : public PhysicalOperator {

public:
    ReadList(const uint64_t& inDataChunkPos, const uint64_t& inValueVectorPos, BaseLists* lists,
        unique_ptr<PhysicalOperator> prevOperator);

    ~ReadList() { lists->reclaim(handle); }

protected:
    void readValuesFromList();

protected:
    static constexpr uint32_t MAX_TO_READ = 512;

    uint64_t inDataChunkPos;
    uint64_t inValueVectorPos;
    shared_ptr<DataChunk> inDataChunk;
    shared_ptr<NodeIDVector> inNodeIDVector;
    shared_ptr<DataChunk> outDataChunk;
    shared_ptr<ValueVector> outValueVector;

    BaseLists* lists;
    unique_ptr<ColumnOrListsHandle> handle;
};

} // namespace processor
} // namespace graphflow
