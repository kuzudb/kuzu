#pragma once

#include "src/processor/include/physical_plan/operator/result/result_set.h"
#include "src/processor/include/physical_plan/operator/result/tuple.h"

using namespace graphflow::common;

namespace graphflow {
namespace processor {

class ResultSetIterator {
public:
    explicit ResultSetIterator(ResultSet* resultSet) : resultSet(resultSet), numIteratedTuples(0) {
        reset();
    }

    void setResultSet(ResultSet* resultSet) {
        this->resultSet = resultSet;
        reset();
    }

    bool hasNextTuple();
    void getNextTuple(Tuple& tuple);

public:
    vector<DataType> dataTypes;

private:
    void reset();
    bool updateTuplePositions(int64_t chunkIdx);
    void updateTuplePositions();
    void setDataChunksTypes();

    ResultSet* resultSet;

    uint64_t numIteratedTuples;
    vector<uint64_t> tuplePositions;
};

} // namespace processor
} // namespace graphflow
