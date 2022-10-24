#pragma once

#include "src/processor/result/include/result_set.h"
#include "src/processor/result/include/result_set_descriptor.h"

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

protected:
    unique_ptr<ResultSetDescriptor> resultSetDescriptor;
};

} // namespace processor
} // namespace graphflow
