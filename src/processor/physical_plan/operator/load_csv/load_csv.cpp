#include "src/processor/include/physical_plan/operator/load_csv/load_csv.h"

#include <cassert>

#include "src/common/include/gf_string.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

LoadCSV::LoadCSV(const string& fname, char tokenSeparator, vector<DataType> csvColumnDataTypes,
    uint32_t totalNumDataChunks, uint32_t outDataChunkSize, uint32_t outDataChunkPos,
    vector<uint32_t> outValueVectorsPos, ExecutionContext& context, uint32_t id)
    : PhysicalOperator{LOAD_CSV, context, id}, fname{fname}, tokenSeparator{tokenSeparator},
      csvColumnDataTypes{csvColumnDataTypes}, reader{fname, tokenSeparator},
      totalNumDataChunks{totalNumDataChunks}, outDataChunkSize{outDataChunkSize},
      outDataChunkPos{outDataChunkPos}, outValueVectorsPos{move(outValueVectorsPos)} {
    resultSet = make_shared<ResultSet>(totalNumDataChunks);
    outDataChunk = make_shared<DataChunk>(outDataChunkSize);
    for (auto& csvColumnDataType : csvColumnDataTypes) {
        outValueVectors.emplace_back(new ValueVector(context.memoryManager, csvColumnDataType));
    }
    for (auto i = 0u; i < outValueVectors.size(); ++i) {
        outDataChunk->insert(this->outValueVectorsPos[i], outValueVectors[i]);
    }
    resultSet->insert(outDataChunkPos, outDataChunk);
    // skip the file header.
    if (reader.hasNextLine()) {
        reader.skipLine();
    }
}

void LoadCSV::getNextTuples() {
    metrics->executionTime.start();
    auto lineIdx = 0ul;
    while (lineIdx < DEFAULT_VECTOR_CAPACITY && reader.hasNextLine()) {
        auto tokenIdx = 0ul;
        while (reader.hasNextToken()) {
            auto& vector = *outValueVectors[tokenIdx];
            auto vectorDataType = csvColumnDataTypes[tokenIdx];
            switch (vectorDataType) {
            case INT64: {
                ((int64_t*)vector.values)[lineIdx] =
                    reader.skipTokenIfNull() ? NULL_INT64 : reader.getInt64();
                break;
            }
            case DOUBLE: {
                ((double*)vector.values)[lineIdx] =
                    reader.skipTokenIfNull() ? NULL_DOUBLE : reader.getDouble();
                break;
            }
            case BOOL: {
                ((uint8_t*)vector.values)[lineIdx] =
                    reader.skipTokenIfNull() ? NULL_BOOL : reader.getBoolean();
                break;
            }
            case STRING: {
                auto strToken = reader.skipTokenIfNull() ? string() : string(reader.getString());
                vector.addString(lineIdx, strToken);
                break;
            }
            default:
                assert(false);
            }
            tokenIdx++;
            if (tokenIdx >= csvColumnDataTypes.size()) {
                reader.skipLine();
                break;
            }
        }
        lineIdx++;
    }
    outDataChunk->state->selectedSize = lineIdx;
    metrics->executionTime.stop();
    metrics->numOutputTuple.increase(outDataChunk->state->selectedSize);
}

} // namespace processor
} // namespace graphflow
