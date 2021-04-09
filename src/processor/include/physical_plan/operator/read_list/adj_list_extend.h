#pragma once

#include "src/processor/include/physical_plan/operator/read_list/read_list.h"

namespace graphflow {
namespace processor {

class AdjListExtend : public ReadList {

public:
    AdjListExtend(uint64_t inDataChunkPos, uint64_t inValueVectorPos, BaseLists* lists,
        unique_ptr<PhysicalOperator> prevOperator);
};

} // namespace processor
} // namespace graphflow
