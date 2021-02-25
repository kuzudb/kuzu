#pragma once

#include "src/processor/include/operator/physical/list_reader/adj_list_extend.h"

namespace graphflow {
namespace processor {

class AdjListOnlyExtend : public AdjListExtend {

public:
    AdjListOnlyExtend(const uint64_t& inDataChunkPos, const uint64_t& inValueVectorPos,
        BaseLists* lists, shared_ptr<ListSyncer> listSyncer, unique_ptr<Operator> prevOperator)
        : AdjListExtend{inDataChunkPos, inValueVectorPos, lists, listSyncer, move(prevOperator)} {};

    void getNextTuples() override;

    unique_ptr<Operator> clone() override {
        return make_unique<AdjListOnlyExtend>(inDataChunkPos, inValueVectorPos, lists,
            handle->getListSyncer(), prevOperator->clone());
    }
};

} // namespace processor
} // namespace graphflow
