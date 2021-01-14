#pragma once

#include "src/processor/include/operator/physical/list_reader/list_reader.h"

namespace graphflow {
namespace processor {

class RelPropertyListReader : public ListReader {

public:
    RelPropertyListReader(const uint64_t& dataChunkPos, const uint64_t& valueVectorPos,
        BaseLists* lists, unique_ptr<Operator> prevOperator);

    void getNextTuples() override;

    unique_ptr<Operator> clone() override {
        return make_unique<RelPropertyListReader>(
            dataChunkPos, valueVectorPos, lists, prevOperator->clone());
    }

private:
    shared_ptr<ValueVector> outValueVector;
};

} // namespace processor
} // namespace graphflow
