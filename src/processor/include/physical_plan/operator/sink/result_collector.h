#pragma once

#include "src/processor/include/physical_plan/operator/result/result_set_iterator.h"
#include "src/processor/include/physical_plan/operator/sink/sink.h"
#include "src/processor/include/physical_plan/query_result.h"

namespace graphflow {
namespace processor {

class ResultCollector : public Sink {

public:
    explicit ResultCollector(shared_ptr<ResultSet> resultSet,
        unique_ptr<PhysicalOperator> prevOperator, PhysicalOperatorType operatorType,
        ExecutionContext& context, uint32_t id)
        : Sink{move(resultSet), move(prevOperator), operatorType, context, id},
          queryResult{make_unique<QueryResult>()} {}

    void reInitialize() override;

    void execute() override;

    unique_ptr<PhysicalOperator> clone() override {
        auto clonedResultSet = make_shared<ResultSet>(resultSet->dataChunks.size());
        for (auto i = 0u; i < resultSet->dataChunks.size(); ++i) {
            clonedResultSet->insert(
                i, make_shared<DataChunk>(resultSet->dataChunks[i]->valueVectors.size()));
        }
        return make_unique<ResultCollector>(
            move(clonedResultSet), prevOperator->clone(), operatorType, context, id);
    }

public:
    unique_ptr<QueryResult> queryResult;

private:
    void resetStringBuffer();
};

} // namespace processor
} // namespace graphflow
