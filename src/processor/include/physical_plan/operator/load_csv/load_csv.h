#pragma once

#include "src/common/include/csv_reader/csv_reader.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

class LoadCSV : public PhysicalOperator {

public:
    LoadCSV(const string& fname, char tokenSeparator, vector<DataType> csvColumnDataTypes,
        uint32_t totalNumDataChunks, uint32_t outDataChunkSize, uint32_t outDataChunkPos,
        vector<uint32_t> outValueVectorsPos, ExecutionContext& context, uint32_t id);

    void reInitialize() override {}

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<LoadCSV>(fname, tokenSeparator, csvColumnDataTypes, totalNumDataChunks,
            outDataChunkSize, outDataChunkPos, outValueVectorsPos, context, id);
    }

private:
    string fname;
    char tokenSeparator;
    vector<DataType> csvColumnDataTypes;
    CSVReader reader;

    uint32_t totalNumDataChunks;
    uint32_t outDataChunkSize;
    uint32_t outDataChunkPos;
    vector<uint32_t> outValueVectorsPos;

    shared_ptr<DataChunk> outDataChunk;
    vector<shared_ptr<ValueVector>> outValueVectors;
};

} // namespace processor
} // namespace graphflow
