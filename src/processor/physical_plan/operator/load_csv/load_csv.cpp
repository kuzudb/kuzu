#include "src/processor/include/physical_plan/operator/load_csv/load_csv.h"

#include <cassert>

#include "src/common/include/string.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

template<bool IS_OUT_DATACHUNK_FILTERED>
LoadCSV<IS_OUT_DATACHUNK_FILTERED>::LoadCSV(
    string fname, char tokenSeparator, vector<DataType> csvColumnDataTypes)
    : PhysicalOperator{LOAD_CSV}, fname{fname}, tokenSeparator{tokenSeparator},
      reader{fname, tokenSeparator}, csvColumnDataTypes{csvColumnDataTypes} {
    resultSet = make_shared<ResultSet>();
    outDataChunk =
        make_shared<DataChunk>(!IS_OUT_DATACHUNK_FILTERED /* initializeSelectedValuesPos */);
    for (auto tokenIdx = 0u; tokenIdx < csvColumnDataTypes.size(); tokenIdx++) {
        outValueVectors.emplace_back(new ValueVector(csvColumnDataTypes[tokenIdx]));
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

template<bool IS_OUT_DATACHUNK_FILTERED>
void LoadCSV<IS_OUT_DATACHUNK_FILTERED>::getNextTuples() {
    auto lineIdx = 0ul;
    while (lineIdx < MAX_VECTOR_SIZE && reader.hasNextLine()) {
        auto tokenIdx = 0ul;
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
    outDataChunk->state->numSelectedValues = lineIdx;
    if constexpr (IS_OUT_DATACHUNK_FILTERED) {
        for (auto i = 0u; i < outDataChunk->state->size; i++) {
            outDataChunk->state->selectedValuesPos[i] = i;
        }
    }
}
template class LoadCSV<true>;
template class LoadCSV<false>;
} // namespace processor
} // namespace graphflow
