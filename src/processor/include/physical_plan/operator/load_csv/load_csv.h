#pragma once

#include "src/common/include/csv_reader/csv_reader.h"
#include "src/processor/include/physical_plan/operator/physical_operator.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

template<bool IS_OUT_DATACHUNK_FILTERED>
class LoadCSV : public PhysicalOperator {

public:
    LoadCSV(string fname, char tokenSeparator, vector<DataType> csvColumnDataTypes,
        ExecutionContext& context, uint32_t id);

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<LoadCSV<IS_OUT_DATACHUNK_FILTERED>>(
            fname, tokenSeparator, csvColumnDataTypes, context, id);
    }

private:
    string fname;
    char tokenSeparator;
    CSVReader reader;
    shared_ptr<DataChunk> outDataChunk;

    vector<DataType> csvColumnDataTypes;
    vector<shared_ptr<ValueVector>> outValueVectors;
};

} // namespace processor
} // namespace graphflow
