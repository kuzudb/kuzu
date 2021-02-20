#pragma once

#include "src/processor/include/operator/physical/list_reader/list_reader.h"

namespace graphflow {
namespace processor {

class RelPropertyListReader : public ListReader {

public:
    RelPropertyListReader(const uint64_t& inDataChunkPos, const uint64_t& inValueVectorPos,
        const uint64_t& outDataChunkPos, BaseLists* lists, shared_ptr<ListSyncer> listSyncer,
        unique_ptr<Operator> prevOperator);

    void getNextTuples() override;

    unique_ptr<Operator> clone() override {
        return make_unique<RelPropertyListReader>(inDataChunkPos, inValueVectorPos, outDataChunkPos,
            lists, handle->getListSyncer(), prevOperator->clone());
    }

private:
    uint64_t outDataChunkPos;
};

} // namespace processor
} // namespace graphflow
