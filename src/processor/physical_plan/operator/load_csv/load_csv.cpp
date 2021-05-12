#include "src/processor/include/physical_plan/operator/load_csv/load_csv.h"

#include <cassert>

using namespace graphflow::common;

namespace graphflow {
namespace processor {

LoadCSV::LoadCSV(string fname, char tokenSeparator, vector<DataType> csvColumnDataTypes)
    : PhysicalOperator{LOAD_CSV}, fname{fname}, tokenSeparator{tokenSeparator},
      reader{fname, tokenSeparator}, csvColumnDataTypes{csvColumnDataTypes} {
    resultSet = make_shared<ResultSet>();
    outDataChunk = make_shared<DataChunk>(false /* initializeSelectedValuesPos */);
    for (auto tokenIdx = 0u; tokenIdx < csvColumnDataTypes.size(); tokenIdx++) {
        outValueVectors.emplace_back(new ValueVector(csvColumnDataTypes[tokenIdx]));
    }
    // skip the file header.
    if (reader.hasNextLine()) {
        reader.skipLine();
    }
}

void LoadCSV::getNextTuples() {
    auto lineIdx = 0ul;
    while (lineIdx < MAX_VECTOR_SIZE && reader.hasNextLine()) {
        auto tokenIdx = 0;
        while (reader.hasNextToken()) {
            auto& vector = *outValueVectors[tokenIdx];
            auto vectorDataType = csvColumnDataTypes[tokenIdx];
            switch (vectorDataType) {
            case INT32: {
                ((int*)vector.values)[lineIdx] =
                    reader.skipTokenIfNull() ? NULL_INT32 : reader.getInt32();
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
                auto strVal = &((gf_string_t*)vector.values)[lineIdx];
                strVal->set(reader.skipTokenIfNull() ? string(&gf_string_t::EMPTY_STRING) :
                                                       string(reader.getString()));
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
    outDataChunk->state->size = lineIdx;
}

unique_ptr<PhysicalOperator> LoadCSV::clone() {
    return make_unique<LoadCSV>(fname, tokenSeparator, csvColumnDataTypes);
}

} // namespace processor
} // namespace graphflow
