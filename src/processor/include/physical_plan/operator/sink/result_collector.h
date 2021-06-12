#pragma once

#include "src/processor/include/physical_plan/operator/result/result_set_iterator.h"
#include "src/processor/include/physical_plan/operator/sink/sink.h"
#include "src/processor/include/physical_plan/query_result.h"

namespace graphflow {
namespace processor {

class ResultCollector : public Sink {

public:
    explicit ResultCollector(unique_ptr<PhysicalOperator> prevOperator,
        PhysicalOperatorType operatorType, ExecutionContext& context, uint32_t id)
        : Sink{move(prevOperator), operatorType, context, id} {
        resultSet = this->prevOperator->getResultSet();
        resultSetIterator = make_unique<ResultSetIterator>(*resultSet);
        queryResult = make_unique<QueryResult>();
    };

    void getNextTuples() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ResultCollector>(prevOperator->clone(), operatorType, context, id);
    }

public:
    unique_ptr<QueryResult> queryResult;

private:
    unique_ptr<ResultSetIterator> resultSetIterator;
};

} // namespace processor
} // namespace graphflow
