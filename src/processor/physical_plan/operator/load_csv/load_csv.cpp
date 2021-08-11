#include "src/processor/include/physical_plan/operator/load_csv/load_csv.h"

#include <cassert>

#include "src/common/include/gf_string.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

LoadCSV::LoadCSV(const string& fname, char tokenSeparator, vector<DataType> csvColumnDataTypes,
    ExecutionContext& context, uint32_t id)
    : PhysicalOperator{LOAD_CSV, context, id}, fname{fname}, tokenSeparator{tokenSeparator},
      reader{fname, tokenSeparator}, csvColumnDataTypes{csvColumnDataTypes} {
    resultSet = make_shared<ResultSet>();
    outDataChunk = make_shared<DataChunk>();
    for (auto tokenIdx = 0u; tokenIdx < csvColumnDataTypes.size(); tokenIdx++) {
        outValueVectors.emplace_back(
            new ValueVector(context.memoryManager, csvColumnDataTypes[tokenIdx]));
    }
    for (auto& outValueVector : outValueVectors) {
        outDataChunk->append(outValueVector);
    }
    resultSet->append(outDataChunk);
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
