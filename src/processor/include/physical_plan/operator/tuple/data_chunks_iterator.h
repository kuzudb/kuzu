#pragma once

#include "src/common/include/literal.h"
#include "src/processor/include/physical_plan/operator/tuple/data_chunks.h"
#include "src/processor/include/physical_plan/operator/tuple/tuple.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

class DataChunksIterator {
public:
    explicit DataChunksIterator(DataChunks& dataChunks)
        : dataChunks(dataChunks), numIteratedTuples(0) {
        initializeTuplePositions();
    }

    void setDataChunks(DataChunks& dataChunks) {
        this->dataChunks = dataChunks;
        initializeTuplePositions();
        numIteratedTuples = 0;
    }

    bool hasNextTuple();
    void getNextTuple(Tuple& tuple);

private:
    void initializeTuplePositions();
    bool updateTuplePositions(int64_t chunkIdx);
    void updateTuplePositions();

    DataChunks& dataChunks;

    uint64_t numIteratedTuples;
    vector<uint64_t> tuplePositions;
};

} // namespace processor
} // namespace graphflow
