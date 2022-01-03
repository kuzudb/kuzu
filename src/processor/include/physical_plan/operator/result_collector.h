#pragma once

#include "src/processor/include/physical_plan/operator/sink.h"
#include "src/processor/include/physical_plan/result/query_result.h"
#include "src/processor/include/physical_plan/result/result_set_iterator.h"

namespace graphflow {
namespace processor {

class ResultCollector : public Sink {

public:
    explicit ResultCollector(vector<DataPos> vectorsToCollectPos,
        unique_ptr<PhysicalOperator> child, ExecutionContext& context, uint32_t id)
        : Sink{move(child), context, id},
          queryResult{make_unique<QueryResult>(vectorsToCollectPos)}, vectorsToCollectPos{move(
                                                                          vectorsToCollectPos)} {}

    PhysicalOperatorType getOperatorType() override { return RESULT_COLLECTOR; }

    shared_ptr<ResultSet> initResultSet() override;

    void execute() override;

    unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ResultCollector>(vectorsToCollectPos, children[0]->clone(), context, id);
    }

public:
    unique_ptr<QueryResult> queryResult;

private:
    void resetStringBuffer();

private:
    vector<DataPos> vectorsToCollectPos;
};

} // namespace processor
} // namespace graphflow
