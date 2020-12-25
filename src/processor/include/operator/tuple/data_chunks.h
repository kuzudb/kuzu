#pragma once

#include "src/common/include/vector/node_vector.h"
#include "src/processor/include/operator/tuple/data_chunk.h"

namespace graphflow {
namespace processor {

class DataChunks {

public:
    shared_ptr<ValueVector> getValueVectorAndSetDataChunk(
        string name, shared_ptr<DataChunk>& outDataChunk) {
        for (auto dataChunk : dataChunks) {
            for (auto& valueVector : dataChunk->valueVectors) {
                if (name == valueVector->getName()) {
                    outDataChunk = dataChunk;
                    return valueVector;
                }
            }
        }
        throw invalid_argument("name not found in dataChunk.");
    }

    void append(shared_ptr<DataChunk> dataChunk) { dataChunks.push_back(dataChunk); }

    uint64_t getNumTuples() {
        uint64_t numTuples = 1;
        for (auto& dataChunk : dataChunks) {
            numTuples *= dataChunk->size;
        }
        return numTuples;
    }

private:
    vector<shared_ptr<DataChunk>> dataChunks;
};

} // namespace processor
} // namespace graphflow
