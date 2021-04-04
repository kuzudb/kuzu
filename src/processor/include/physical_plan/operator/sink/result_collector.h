#pragma once

#include "src/processor/include/physical_plan/operator/sink/sink.h"
#include "src/processor/include/physical_plan/operator/tuple/data_chunks_iterator.h"
#include "src/processor/include/physical_plan/query_result.h"

namespace graphflow {
namespace processor {

class ResultCollector : public Sink {

public:
    explicit ResultCollector(unique_ptr<PhysicalOperator> prevOperator,
        PhysicalOperatorType operatorType = RESULT_COLLECTOR)
        : Sink{move(prevOperator), operatorType} {
        dataChunks = this->prevOperator->getDataChunks();
        dataChunksIterator = make_unique<DataChunksIterator>(*dataChunks);
        queryResult = make_unique<QueryResult>();
    };

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ResultCollector>(prevOperator->clone(), operatorType);
    }

public:
    unique_ptr<QueryResult> queryResult;

private:
    unique_ptr<DataChunksIterator> dataChunksIterator;
};

} // namespace processor
} // namespace graphflow
