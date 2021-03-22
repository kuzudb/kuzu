#pragma once

#include "src/processor/include/physical_plan/operator/list_reader/list_reader.h"

namespace graphflow {
namespace processor {

class AdjListExtend : public ListReader {

public:
    AdjListExtend(const uint64_t& inDataChunkIdx, const uint64_t& inValueVectorIdx,
        BaseLists* lists, unique_ptr<PhysicalOperator> prevOperator);
};

} // namespace processor
} // namespace graphflow
