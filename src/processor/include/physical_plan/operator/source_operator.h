#pragma once

#include "src/processor/include/physical_plan/result/result_set.h"
#include "src/processor/include/physical_plan/result/result_set_descriptor.h"

namespace graphflow {
namespace processor {

class SourceOperator {

protected:
    explicit SourceOperator(unique_ptr<ResultSetDescriptor> resultSetDescriptor)
        : resultSetDescriptor{move(resultSetDescriptor)} {}

    shared_ptr<ResultSet> populateResultSet() {
        auto numDataChunks = resultSetDescriptor->getNumDataChunks();
        auto resultSet = make_shared<ResultSet>(numDataChunks);
        for (auto i = 0u; i < numDataChunks; ++i) {
            auto dataChunkDescriptor = resultSetDescriptor->getDataChunkDescriptor(i);
            resultSet->insert(i, make_shared<DataChunk>(dataChunkDescriptor->getNumValueVectors()));
        }
        return resultSet;
    }

    void reInitToRerunSubPlan(ResultSet& resultSet) {
        for (auto& dataChunk : resultSet.dataChunks) {
            dataChunk->state->initOriginalAndSelectedSize(0);
            dataChunk->state->selVector->resetSelectorToUnselected();
            dataChunk->state->currIdx = -1;
        }
    }

protected:
    unique_ptr<ResultSetDescriptor> resultSetDescriptor;
};

} // namespace processor
} // namespace graphflow
